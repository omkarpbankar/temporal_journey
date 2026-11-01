"""The capstone under fire. Each test injects a failure from the plan's chaos list."""

import asyncio
import base64
import os
import subprocess
import sys
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from temporalio import workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.exceptions import is_cancelled_exception
from temporalio.worker import Replayer, Worker
from temporalio.workflow import NondeterminismError

from activities.commerce import Fault
from capstone.activities import CapstoneActivities
from capstone.api import build_api, workflow_id
from capstone.models import ORCHESTRATION, ProcessingRequest
from capstone.worker import build_worker
from capstone.workflows import ChunkProcessingWorkflow, DataRequestWorkflow
from shared.encryption import EncryptionCodec, encrypted_converter
from shared.ledger import Ledger
from shared.object_store import ObjectStore
from tests.helpers.history import event_names

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[1]
ROLES = ("orchestration", "io", "data")
TOKEN = "test-token"


class Rig:
    def __init__(self, client, queue, tmp_path, faults=None, **activity_options):
        self.client = client
        self.queue = queue
        self.prefix = f"{queue}-"
        self.dir = tmp_path
        self.ledger = Ledger(tmp_path / "ledger.db")
        self.store = ObjectStore(tmp_path / "store")
        self.activities = CapstoneActivities(self.store, self.ledger, faults or {}, **activity_options)
        self.workers = []

    async def __aenter__(self):
        self.workers = [build_worker(self.client, role, self.activities, self.prefix) for role in ROLES]
        for worker in self.workers:
            await worker.__aenter__()
        return self

    async def __aexit__(self, *exc):
        for worker in self.workers:
            await worker.__aexit__(*exc)

    def request(self, **options) -> ProcessingRequest:
        settings = dict(activity_timeout_seconds=2, notify_email="ops@example.com", source_name="sales")
        settings.update(options)
        return ProcessingRequest(request_id=self.queue, queue_prefix=self.prefix, **settings)

    async def start(self, **options):
        return await self.client.start_workflow(
            DataRequestWorkflow.run,
            self.request(**options),
            id=workflow_id(self.queue),
            task_queue=self.prefix + ORCHESTRATION,
        )


async def wait_for_state(handle, state):
    for _ in range(400):
        if (await handle.query(DataRequestWorkflow.status)).state == state:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"never reached {state}")


def assert_one_of_each(rig):
    for kind in ("record", "metadata", "event", "notification"):
        assert rig.ledger.count(kind, rig.queue) == 1, kind


async def test_happy_path(local_env, queue, tmp_path):
    async with Rig(local_env.client, queue, tmp_path) as rig:
        result = await (await rig.start()).result()
    assert (result.state, result.rows, result.quality) == ("COMPLETED", 100, 1.0)
    assert rig.store.exists(f"results/{queue}.json")
    assert_one_of_each(rig)


async def test_chunking_uses_continue_as_new(local_env, queue, tmp_path):
    client = local_env.client
    async with Rig(client, queue, tmp_path) as rig:
        handle = await rig.start(rows=100, chunk_size=10, chunks_per_run=3)
        await handle.result()
    child = f"{workflow_id(queue)}-chunks"
    runs = [run async for run in client.list_workflows(f"WorkflowId = '{child}'")]
    assert len(runs) >= 3


@pytest.mark.parametrize(
    "activity_name, fault",
    [
        ("download_source", Fault("transient", 2)),  # network failure
        ("publish_event", Fault("transient", 3)),  # API failure
        ("create_record", Fault("transient", 2)),  # database failure
        ("store_results", Fault("lost_response", 1)),  # duplicate execution
        ("update_metadata", Fault("lost_response", 2)),  # duplicate execution
        ("notify_user", Fault("lost_response", 1)),  # duplicate execution
        ("process_chunk", Fault("hang", 1, seconds=1.5)),  # timeout
    ],
    ids=["network", "api", "database", "dup-store", "dup-metadata", "dup-notify", "timeout"],
)
async def test_recovers_from_injected_failure(local_env, queue, tmp_path, activity_name, fault):
    async with Rig(local_env.client, queue, tmp_path, {activity_name: fault}) as rig:
        result = await (await rig.start(activity_timeout_seconds=1)).result()
    assert result.state == "COMPLETED"
    assert_one_of_each(rig)


@pytest.mark.parametrize("activity_name", ["store_results", "update_metadata", "publish_event"])
async def test_worker_process_killed_after_commit(local_env, queue, tmp_path, activity_name):
    client = local_env.client
    rig = Rig(client, queue, tmp_path)
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tests.helpers.capstone_crash_worker",
            client.service_client.config.target_host,
            rig.prefix,
            str(tmp_path),
            activity_name,
        ],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": f"{ROOT / 'src'}{os.pathsep}{ROOT}"},
    )
    handle = await rig.start()
    assert await asyncio.to_thread(process.wait, 60) == 1

    async with rig:
        result = await asyncio.wait_for(handle.result(), 60)
    assert result.state == "COMPLETED"
    assert_one_of_each(rig)


async def test_source_delay_uses_durable_timers(time_env, queue, tmp_path):
    async with Rig(time_env.client, queue, tmp_path, source_delay_checks=3) as rig:
        handle = await rig.start()
        result = await handle.result()
    assert result.state == "COMPLETED"
    assert (await event_names(handle)).count("TIMER_FIRED") == 3


async def test_low_quality_waits_for_a_human(local_env, queue, tmp_path):
    async with Rig(local_env.client, queue, tmp_path) as rig:
        handle = await rig.start(null_fraction=0.5)
        await wait_for_state(handle, "AWAITING_APPROVAL")
        assert not rig.store.exists(f"results/{queue}.json")
        await handle.signal(DataRequestWorkflow.decide, True)
        assert (await handle.result()).state == "COMPLETED"


async def test_rejection_removes_staged_data(local_env, queue, tmp_path):
    async with Rig(local_env.client, queue, tmp_path) as rig:
        handle = await rig.start(null_fraction=0.5)
        await wait_for_state(handle, "AWAITING_APPROVAL")
        await handle.signal(DataRequestWorkflow.decide, False)
        assert (await handle.result()).state == "REJECTED"
    assert not rig.store.exists(f"{queue}/staged.json")


async def test_cancellation_cleans_up(local_env, queue, tmp_path):
    async with Rig(local_env.client, queue, tmp_path) as rig:
        handle = await rig.start(null_fraction=0.5)
        await wait_for_state(handle, "AWAITING_APPROVAL")
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as error:
            await handle.result()
    assert is_cancelled_exception(error.value.cause)
    assert not rig.store.exists(f"{queue}/staged.json")
    assert rig.ledger.count("cleanup", queue) == 1


async def test_exhausted_retries_fail_cleanly_and_tell_the_user(local_env, queue, tmp_path):
    faults = {"store_results": Fault("transient", 99)}
    async with Rig(local_env.client, queue, tmp_path, faults) as rig:
        handle = await rig.start()
        with pytest.raises(WorkflowFailureError):
            await handle.result()
        status = await handle.query(DataRequestWorkflow.status)
    assert status.state == "FAILED"
    assert rig.ledger.count("cleanup", queue) == 1
    assert rig.ledger.count("notification", queue) == 1


async def test_invalid_request_fails_before_any_side_effect(local_env, queue, tmp_path):
    async with Rig(local_env.client, queue, tmp_path) as rig:
        handle = await rig.start(rows=0)
        with pytest.raises(WorkflowFailureError):
            await handle.result()
    assert rig.ledger.count("record") == 0


async def test_update_changes_who_is_notified(local_env, queue, tmp_path):
    async with Rig(local_env.client, queue, tmp_path) as rig:
        handle = await rig.start(null_fraction=0.5)
        await wait_for_state(handle, "AWAITING_APPROVAL")
        previous = await handle.execute_update(DataRequestWorkflow.change_notify_email, "new@example.com")
        await handle.signal(DataRequestWorkflow.decide, True)
        await handle.result()
    assert previous == "ops@example.com"


async def test_encrypted_end_to_end(local_env, queue, tmp_path):
    config = local_env.client.config()
    config["data_converter"] = encrypted_converter(EncryptionCodec({"key-1": os.urandom(32)}, "key-1"))
    async with Rig(Client(**config), queue, tmp_path) as rig:
        handle = await rig.start(notify_email="private@example.com")
        assert (await handle.result()).state == "COMPLETED"
    raw_handle = local_env.client.get_workflow_handle(workflow_id(queue))
    raw = b"".join([event.SerializeToString() async for event in raw_handle.fetch_history_events()])
    assert b"private@example.com" not in raw


@workflow.defn(name="DataRequestWorkflow", sandboxed=False)
class BeforeAuditStep(DataRequestWorkflow):
    """The code as it was before the audit step was added."""

    audit_enabled = False

    @workflow.run
    async def run(self, request: ProcessingRequest):
        return await super().run(request)


class UnpatchedAudit(DataRequestWorkflow):
    async def _audit(self, request):
        from datetime import timedelta

        await workflow.execute_activity_method(
            CapstoneActivities.write_audit, request.request_id, start_to_close_timeout=timedelta(seconds=5)
        )


@workflow.defn(name="DataRequestWorkflow", sandboxed=False)
class AuditWithoutPatch(UnpatchedAudit):
    @workflow.run
    async def run(self, request: ProcessingRequest):
        return await super().run(request)


async def test_deployment_change_is_safe_with_a_patch(local_env, queue, tmp_path):
    client = local_env.client
    rig = Rig(client, queue, tmp_path)
    workers = [build_worker(client, role, rig.activities, rig.prefix) for role in ("io", "data")]
    orchestration = Worker(
        client,
        task_queue=rig.prefix + ORCHESTRATION,
        workflows=[BeforeAuditStep, ChunkProcessingWorkflow],
    )
    async with workers[0], workers[1], orchestration:
        handle = await rig.start()
        await handle.result()
    history = await handle.fetch_history()

    # Today's code replays the old run, and it does not start auditing the past.
    await Replayer(workflows=[DataRequestWorkflow]).replay_workflow(history)
    # The same change without patched() would have broken every in-flight run.
    with pytest.raises(NondeterminismError):
        await Replayer(workflows=[AuditWithoutPatch]).replay_workflow(history)


@pytest_asyncio.fixture
async def api(local_env, queue, tmp_path):
    async with Rig(local_env.client, queue, tmp_path) as rig:
        app = build_api(local_env.client, TOKEN, rig.prefix, activity_timeout_seconds=2)
        transport = httpx.ASGITransport(app=app)
        headers = {"Authorization": f"Bearer {TOKEN}"}
        async with httpx.AsyncClient(transport=transport, base_url="http://test", headers=headers) as http:
            yield http, rig


BODY = {"source_name": "sales", "notify_email": "ops@example.com"}


async def test_api_needs_a_token(api, queue):
    http, _ = api
    response = await http.post("/requests", json={**BODY, "request_id": queue}, headers={"Authorization": ""})
    assert response.status_code == 401


async def test_api_full_journey_with_approval(api, queue, local_env):
    http, rig = api
    body = {**BODY, "request_id": queue, "null_fraction": 0.5}
    assert (await http.post("/requests", json=body)).status_code == 202

    handle = local_env.client.get_workflow_handle(workflow_id(queue))
    await wait_for_state(handle, "AWAITING_APPROVAL")
    assert (await http.get(f"/requests/{queue}")).json()["state"] == "AWAITING_APPROVAL"

    change = await http.patch(f"/requests/{queue}/notify-email", json={"email": "not-an-email"})
    assert change.status_code == 409

    await http.post(f"/requests/{queue}/decision", json={"approved": True})
    await handle.result()
    assert (await http.get(f"/requests/{queue}")).json()["state"] == "COMPLETED"


async def test_api_retry_does_not_start_a_second_run(api, queue):
    http, _ = api
    body = {**BODY, "request_id": queue, "null_fraction": 0.5}
    first = (await http.post("/requests", json=body)).json()
    second = (await http.post("/requests", json=body)).json()
    assert first["run_id"] == second["run_id"]
    await http.delete(f"/requests/{queue}")


async def test_api_reports_unknown_request(api):
    http, _ = api
    assert (await http.get("/requests/does-not-exist")).status_code == 404
