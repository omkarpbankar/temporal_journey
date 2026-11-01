import asyncio

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError, CancelledError, TerminatedError
from temporalio.worker import Worker

from tests.helpers.history import event_names, workflow_task_failure
from workflows import failure_lab as lab

WORKFLOWS = [
    lab.RetryExhaustedWorkflow,
    lab.NonRetryableWorkflow,
    lab.ActivityTimeoutWorkflow,
    lab.BusinessFailureWorkflow,
    lab.PythonBugWorkflow,
    lab.WaitsForeverWorkflow,
]
ACTIVITIES = [lab.always_fails, lab.rejected, lab.too_slow]


def start_worker(env, queue):
    return Worker(env.client, task_queue=queue, workflows=WORKFLOWS, activities=ACTIVITIES)


async def test_retry_exhaustion_shape(time_env, queue):
    async with start_worker(time_env, queue):
        report = await time_env.client.execute_workflow(lab.RetryExhaustedWorkflow.run, 3, id=queue, task_queue=queue)
    assert (report.outer, report.cause) == ("ActivityError", "ApplicationError")
    assert report.retry_state == "MAXIMUM_ATTEMPTS_REACHED"


async def test_non_retryable_shape(time_env, queue):
    async with start_worker(time_env, queue):
        report = await time_env.client.execute_workflow(lab.NonRetryableWorkflow.run, id=queue, task_queue=queue)
    assert report.retry_state == "NON_RETRYABLE_FAILURE"


async def test_timeout_shape(time_env, queue):
    async with start_worker(time_env, queue):
        report = await time_env.client.execute_workflow(lab.ActivityTimeoutWorkflow.run, id=queue, task_queue=queue)
    print("\nOBSERVED timeout", report)
    assert report.cause == "TimeoutError"


async def test_application_error_fails_the_workflow(time_env, queue):
    async with start_worker(time_env, queue):
        with pytest.raises(WorkflowFailureError) as error:
            await time_env.client.execute_workflow(lab.BusinessFailureWorkflow.run, id=queue, task_queue=queue)
    assert isinstance(error.value.cause, ApplicationError)
    assert error.value.cause.type == "PolicyViolation"


@pytest.mark.integration
async def test_python_bug_keeps_the_workflow_open(local_env, queue):
    async with start_worker(local_env, queue):
        handle = await local_env.client.start_workflow(lab.PythonBugWorkflow.run, id=queue, task_queue=queue)
        message = None
        while message is None:
            message = await workflow_task_failure(handle)
            await asyncio.sleep(0.05)
        assert "typo-level bug" in message
        assert (await handle.describe()).status.name == "RUNNING"
        await handle.terminate()


@pytest.mark.integration
async def test_cancel_reaches_the_workflow(local_env, queue):
    async with start_worker(local_env, queue):
        handle = await local_env.client.start_workflow(lab.WaitsForeverWorkflow.run, id=queue, task_queue=queue)
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as error:
            await handle.result()
    assert isinstance(error.value.cause, CancelledError)


@pytest.mark.integration
async def test_terminate_gives_the_workflow_no_chance(local_env, queue):
    async with start_worker(local_env, queue):
        handle = await local_env.client.start_workflow(lab.WaitsForeverWorkflow.run, id=queue, task_queue=queue)
        await handle.terminate("operator decision")
        with pytest.raises(WorkflowFailureError) as error:
            await handle.result()
    assert isinstance(error.value.cause, TerminatedError)
    assert "WORKFLOW_EXECUTION_TERMINATED" in await event_names(handle)
