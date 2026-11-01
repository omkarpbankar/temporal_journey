import asyncio
import socket
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import pytest
from temporalio.client import Client
from temporalio.worker import Worker

from activities.commerce import CommerceActivities, Fault
from shared.diagnose import RETRY_STORM_ATTEMPTS, diagnose
from shared.models import CheckoutInput, Order
from tests.helpers.history import event_names
from workers.observed_worker import metrics_runtime
from workflows import versioned as v
from workflows.checkout import CheckoutWorkflow

pytestmark = pytest.mark.integration


def checkout_input(max_attempts=0):
    return CheckoutInput(Order("o-1", "c-1", "keyboard", 1, 5000), max_attempts=max_attempts)


def checkout_worker(client, queue, ledger, faults=None):
    activities = CommerceActivities(ledger, faults or {})
    return Worker(
        client,
        task_queue=queue,
        workflows=[CheckoutWorkflow],
        activities=[activities.charge_payment, activities.reserve_inventory, activities.send_email],
        activity_executor=ThreadPoolExecutor(4),
    )


async def eventually(handle, verdict):
    report = None
    for _ in range(200):
        report = await diagnose(handle)
        if report.verdict == verdict:
            return report
        await asyncio.sleep(0.05)
    raise AssertionError(f"wanted {verdict}, last saw {report}")


async def test_incident_no_workflow_worker(local_env, queue):
    handle = await local_env.client.start_workflow(CheckoutWorkflow.run, checkout_input(), id=queue, task_queue=queue)
    report = await eventually(handle, "NO_WORKFLOW_WORKER")
    await handle.terminate()
    assert report.status == "RUNNING"


async def test_incident_no_activity_worker(local_env, queue):
    client = local_env.client
    async with Worker(client, task_queue=queue, workflows=[CheckoutWorkflow]):
        handle = await client.start_workflow(CheckoutWorkflow.run, checkout_input(), id=queue, task_queue=queue)
        report = await eventually(handle, "NO_ACTIVITY_WORKER")
        await handle.terminate()
    assert "charge_payment" in report.evidence[-1]


async def test_incident_retry_storm(local_env, queue, ledger):
    client = local_env.client
    faults = {"charge_payment": Fault("transient", 10_000)}
    async with checkout_worker(client, queue, ledger, faults):
        handle = await client.start_workflow(CheckoutWorkflow.run, checkout_input(), id=queue, task_queue=queue)
        report = await eventually(handle, "RETRY_STORM")
        await handle.terminate()
    assert "temporary outage" in report.evidence[-1]
    assert RETRY_STORM_ATTEMPTS == 5


async def test_incident_nondeterministic_deploy(local_env, queue):
    client = local_env.client
    async with Worker(client, task_queue=queue, workflows=[v.ShippingV1], activities=[v.step_a, v.step_b, v.step_c]):
        handle = await client.start_workflow(v.NAME, 1.0, id=queue, task_queue=queue)
        while "TIMER_STARTED" not in await event_names(handle):
            await asyncio.sleep(0.02)
    async with Worker(client, task_queue=queue, workflows=[v.ShippingV2Unsafe], activities=[v.step_a, v.step_b, v.step_c]):
        report = await eventually(handle, "NONDETERMINISM")
        await handle.terminate()
    print("OBSERVED", report)


async def test_closed_run_is_reported_as_closed(local_env, queue, ledger):
    client = local_env.client
    async with checkout_worker(client, queue, ledger):
        handle = await client.start_workflow(CheckoutWorkflow.run, checkout_input(5), id=queue, task_queue=queue)
        await handle.result()
    assert (await diagnose(handle)).verdict == "CLOSED"


async def test_worker_exposes_prometheus_metrics(local_env, queue, ledger):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    client = await Client.connect(
        local_env.client.service_client.config.target_host,
        runtime=metrics_runtime(f"127.0.0.1:{port}"),
    )
    async with checkout_worker(client, queue, ledger, {"charge_payment": Fault("transient", 2)}):
        await client.execute_workflow(CheckoutWorkflow.run, checkout_input(5), id=queue, task_queue=queue)
        await asyncio.sleep(0.2)
        body = await asyncio.to_thread(
            lambda: urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics").read().decode()
        )
    names = sorted({line.split("{")[0].split(" ")[0] for line in body.splitlines() if line.startswith("temporal_")})
    print("OBSERVED METRICS", [n for n in names if not n.endswith(("_bucket", "_sum"))])
    assert "temporal_activity_execution_failed" in names
    assert "temporal_workflow_completed" in names
