import asyncio
import uuid

import pytest
from temporalio.api.workflowservice.v1 import SetWorkerDeploymentCurrentVersionRequest
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.worker import Replayer, Worker, WorkerDeploymentConfig
from temporalio.workflow import NondeterminismError

from tests.helpers.history import event_names, workflow_task_failure
from workflows import versioned as v

STEPS = [v.step_a, v.step_b, v.step_c]


async def finished_history(env, queue, workflow_class):
    async with Worker(env.client, task_queue=queue, workflows=[workflow_class], activities=STEPS):
        handle = await env.client.start_workflow(v.NAME, 1.0, id=queue, task_queue=queue)
        await handle.result()
    return await handle.fetch_history()


async def replay(workflow_class, history):
    await Replayer(workflows=[workflow_class]).replay_workflow(history)


async def test_inserting_a_step_breaks_replay(time_env, queue):
    history = await finished_history(time_env, queue, v.ShippingV1)
    with pytest.raises(NondeterminismError):
        await replay(v.ShippingV2Unsafe, history)


async def test_patched_code_replays_old_history(time_env, queue):
    await replay(v.ShippingV3Patched, await finished_history(time_env, queue, v.ShippingV1))


async def test_patched_code_runs_the_new_step_for_new_runs(time_env, queue):
    async with Worker(time_env.client, task_queue=queue, workflows=[v.ShippingV3Patched], activities=STEPS):
        result = await time_env.client.execute_workflow(v.NAME, 1.0, id=queue, task_queue=queue)
    assert result == ["a", "c", "b"]


async def test_deprecating_too_early_breaks_old_runs(time_env, queue):
    history = await finished_history(time_env, queue, v.ShippingV1)
    with pytest.raises(NondeterminismError):
        await replay(v.ShippingV4Deprecated, history)


async def test_deprecated_code_replays_patched_history(time_env, queue):
    await replay(v.ShippingV4Deprecated, await finished_history(time_env, queue, v.ShippingV3Patched))


async def test_clean_code_replays_deprecated_history(time_env, queue):
    await replay(v.ShippingV5Clean, await finished_history(time_env, queue, v.ShippingV4Deprecated))


@pytest.mark.integration
async def test_bad_deploy_stalls_a_live_run_and_a_fix_rescues_it(local_env, queue):
    client = local_env.client
    async with Worker(client, task_queue=queue, workflows=[v.ShippingV1], activities=STEPS):
        handle = await client.start_workflow(v.NAME, 1.0, id=queue, task_queue=queue)
        while "TIMER_STARTED" not in await event_names(handle):
            await asyncio.sleep(0.02)

    # Version 1 Workers are gone, and the unsafe version is now deployed.
    async with Worker(client, task_queue=queue, workflows=[v.ShippingV2Unsafe], activities=STEPS):
        message = None
        while message is None:
            message = await workflow_task_failure(handle)
            await asyncio.sleep(0.05)
    print("OBSERVED bad deploy:", message[:120])
    assert (await handle.describe()).status.name == "RUNNING"

    # The run didn't die. Deploying a compatible version lets it finish.
    async with Worker(client, task_queue=queue, workflows=[v.ShippingV3Patched], activities=STEPS):
        assert await asyncio.wait_for(handle.result(), 30) == ["a", "b"]


@pytest.mark.integration
async def test_worker_versioning_keeps_a_pinned_run_on_its_own_build(local_env, queue):
    client = local_env.client
    deployment = f"shipping-{uuid.uuid4().hex[:6]}"

    def versioned_worker(build_id, workflow_class):
        return Worker(
            client,
            task_queue=queue,
            workflows=[workflow_class],
            activities=STEPS,
            deployment_config=WorkerDeploymentConfig(
                version=WorkerDeploymentVersion(deployment, build_id),
                use_worker_versioning=True,
                default_versioning_behavior=VersioningBehavior.PINNED,
            ),
        )

    async def make_current(build_id):
        request = SetWorkerDeploymentCurrentVersionRequest(
            namespace=client.namespace, deployment_name=deployment, build_id=build_id
        )
        for _ in range(100):
            try:
                await client.workflow_service.set_worker_deployment_current_version(request)
                return
            except Exception:
                await asyncio.sleep(0.1)
        raise AssertionError(f"could not make {build_id} current")

    async with versioned_worker("build-1", v.ShippingV1), versioned_worker("build-2", v.ShippingV2Unsafe):
        await make_current("build-1")
        old_run = await client.start_workflow(v.NAME, 2.0, id=f"{queue}-old", task_queue=queue)
        while "TIMER_STARTED" not in await event_names(old_run):
            await asyncio.sleep(0.02)

        await make_current("build-2")
        new_result = await client.execute_workflow(v.NAME, 0.1, id=f"{queue}-new", task_queue=queue)

        # The "unsafe" change is safe here: the old run never meets the new code.
        assert await old_run.result() == ["a", "b"]
    assert new_result == ["a", "c", "b"]
