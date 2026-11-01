import asyncio

import pytest
from temporalio.worker import Worker

from activities.greetings import GreetingInput, compose_greeting
from tests.helpers.history import event_names
from workflows.hello import HelloWorkflow, SlowHelloWorkflow

WORKFLOWS = [HelloWorkflow, SlowHelloWorkflow]


async def test_hello_returns_structured_output(time_env, queue):
    async with Worker(time_env.client, task_queue=queue, workflows=WORKFLOWS, activities=[compose_greeting]):
        result = await time_env.client.execute_workflow(
            HelloWorkflow.run, GreetingInput("Asha", "hi"), id=queue, task_queue=queue
        )
    assert result.greeting == "Namaste, Asha!"
    assert result.workflow_id == queue


async def test_slow_hello_skips_its_timer(time_env, queue):
    async with Worker(time_env.client, task_queue=queue, workflows=WORKFLOWS, activities=[compose_greeting]):
        result = await time_env.client.execute_workflow(
            SlowHelloWorkflow.run, GreetingInput("Asha"), id=queue, task_queue=queue
        )
    assert result == ["Hello, Asha!", "Hola, Asha!"]


async def test_history_tells_the_lifecycle_story(time_env, queue):
    async with Worker(time_env.client, task_queue=queue, workflows=WORKFLOWS, activities=[compose_greeting]):
        handle = await time_env.client.start_workflow(
            HelloWorkflow.run, GreetingInput("Asha"), id=queue, task_queue=queue
        )
        await handle.result()
    names = await event_names(handle)
    assert names[:2] == ["WORKFLOW_EXECUTION_STARTED", "WORKFLOW_TASK_SCHEDULED"]
    assert "ACTIVITY_TASK_SCHEDULED" in names and "ACTIVITY_TASK_COMPLETED" in names
    assert names[-1] == "WORKFLOW_EXECUTION_COMPLETED"


@pytest.mark.integration
async def test_stopping_the_worker_mid_run_loses_nothing(local_env, queue):
    """The Phase 2 lab, automated: the Worker leaves and the run stays alive."""
    client = local_env.client
    async with Worker(client, task_queue=queue, workflows=WORKFLOWS, activities=[compose_greeting]):
        handle = await client.start_workflow(
            SlowHelloWorkflow.run, GreetingInput("Asha"), id=queue, task_queue=queue
        )
        while "TIMER_STARTED" not in await event_names(handle):
            await asyncio.sleep(0.05)
    assert (await handle.describe()).status.name == "RUNNING"
    await handle.terminate()
