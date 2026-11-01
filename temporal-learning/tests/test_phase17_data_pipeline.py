import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import CancelledError
from temporalio.worker import Worker

from activities.data_pipeline import DataActivities
from shared.object_store import ObjectStore
from workflows.data_pipeline import DataPipelineWorkflow, PipelineInput


@pytest.fixture
def activities(tmp_path) -> DataActivities:
    return DataActivities(ObjectStore(tmp_path))


def worker(env, queue, a):
    return Worker(
        env.client,
        task_queue=queue,
        workflows=[DataPipelineWorkflow],
        activities=[a.extract, a.validate, a.transform, a.load_staging, a.quality_check, a.publish, a.delete_staging],
        activity_executor=ThreadPoolExecutor(8),
    )


async def wait_for_stage(handle, stage):
    for _ in range(400):
        if (await handle.query(DataPipelineWorkflow.status)).stage == stage:
            return
        await asyncio.sleep(0.025)
    raise AssertionError(f"never reached {stage}")


async def test_clean_data_publishes_without_a_human(local_env, queue, activities):
    async with worker(local_env, queue, activities):
        result = await local_env.client.execute_workflow(
            DataPipelineWorkflow.run, PipelineInput("run-1", ["p1", "p2", "p3"]), id=queue, task_queue=queue
        )
    assert result.status == "completed"
    assert activities.store.exists("published/run-1.json")


async def test_failed_partition_reruns_alone(local_env, queue, activities):
    activities.broken_partitions = {"p2"}
    async with worker(local_env, queue, activities):
        handle = await local_env.client.start_workflow(
            DataPipelineWorkflow.run, PipelineInput("run-2", ["p1", "p2", "p3"]), id=queue, task_queue=queue
        )
        await wait_for_stage(handle, "awaiting_partition_decision")
        status = await handle.query(DataPipelineWorkflow.status)
        assert status.failed == ["p2"]
        assert sorted(status.done) == ["p1", "p3"]

        activities.broken_partitions = set()  # someone fixed the bug
        await handle.signal(DataPipelineWorkflow.retry_failed_partitions)
        result = await handle.result()
    assert result.status == "completed"
    assert result.abandoned == []


async def test_failed_partition_can_be_dropped(local_env, queue, activities):
    activities.broken_partitions = {"p2"}
    async with worker(local_env, queue, activities):
        handle = await local_env.client.start_workflow(
            DataPipelineWorkflow.run, PipelineInput("run-3", ["p1", "p2"]), id=queue, task_queue=queue
        )
        await wait_for_stage(handle, "awaiting_partition_decision")
        await handle.signal(DataPipelineWorkflow.drop_failed_partitions)
        result = await handle.result()
    assert result.status == "completed"
    assert result.abandoned == ["p2"]


async def test_low_quality_waits_for_approval(local_env, queue, activities):
    async with worker(local_env, queue, activities):
        handle = await local_env.client.start_workflow(
            DataPipelineWorkflow.run, PipelineInput("run-4", ["p1"], null_fraction=0.5), id=queue, task_queue=queue
        )
        await wait_for_stage(handle, "awaiting_approval")
        assert not activities.store.exists("published/run-4.json")
        await handle.signal(DataPipelineWorkflow.decide, True)
        result = await handle.result()
    assert result.status == "completed"
    assert result.quality == 0.5


async def test_rejection_compensates(local_env, queue, activities):
    async with worker(local_env, queue, activities):
        handle = await local_env.client.start_workflow(
            DataPipelineWorkflow.run, PipelineInput("run-5", ["p1"], null_fraction=0.5), id=queue, task_queue=queue
        )
        await wait_for_stage(handle, "awaiting_approval")
        await handle.signal(DataPipelineWorkflow.decide, False)
        result = await handle.result()
    assert result.status == "rejected"
    assert not activities.store.exists("run-5/staging.json")


async def test_no_decision_means_rejected(time_env, queue, activities):
    async with worker(time_env, queue, activities):
        result = await time_env.client.execute_workflow(
            DataPipelineWorkflow.run, PipelineInput("run-6", ["p1"], null_fraction=0.5), id=queue, task_queue=queue
        )
    assert result.status == "rejected"


async def test_cancel_during_approval_compensates(local_env, queue, activities):
    async with worker(local_env, queue, activities):
        handle = await local_env.client.start_workflow(
            DataPipelineWorkflow.run, PipelineInput("run-7", ["p1"], null_fraction=0.5), id=queue, task_queue=queue
        )
        await wait_for_stage(handle, "awaiting_approval")
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as error:
            await handle.result()
    assert isinstance(error.value.cause, CancelledError)
    assert not activities.store.exists("run-7/staging.json")
