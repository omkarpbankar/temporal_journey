import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from temporalio.worker import Replayer, Worker

from activities.commerce import CommerceActivities
from shared.models import Order
from tests.helpers.history import workflow_task_failure
from workflows import determinism_lab as lab
from workflows.order_basics import OrderBasicsWorkflow

ORDER = Order("o-1", "c-1", "keyboard", 1, 5000)


def worker(env, queue, ledger):
    return Worker(
        env.client,
        task_queue=queue,
        workflows=[OrderBasicsWorkflow],
        activities=[CommerceActivities(ledger).charge_payment],
        activity_executor=ThreadPoolExecutor(4),
    )


async def test_invalid_order_is_rejected_without_activities(time_env, queue, ledger):
    async with worker(time_env, queue, ledger):
        result = await time_env.client.execute_workflow(
            OrderBasicsWorkflow.run, Order("o-2", "c", "", 0, 0), id=queue, task_queue=queue
        )
    assert result.status == "rejected"
    assert ledger.count("charge") == 0


async def test_waits_for_signal_then_completes(time_env, queue, ledger):
    async with worker(time_env, queue, ledger):
        handle = await time_env.client.start_workflow(OrderBasicsWorkflow.run, ORDER, id=queue, task_queue=queue)
        await handle.signal(OrderBasicsWorkflow.mark_warehouse_ready)
        result = await handle.result()
    assert result.status == "completed"
    assert ledger.count("charge", "o-1") == 1


async def test_wait_times_out_after_a_day(time_env, queue, ledger):
    async with worker(time_env, queue, ledger):
        result = await time_env.client.execute_workflow(OrderBasicsWorkflow.run, ORDER, id=queue, task_queue=queue)
    assert result.status == "needs_review"


@pytest.mark.integration
@pytest.mark.parametrize(
    "bad, blocked_call",
    [
        (lab.BadRandomWorkflow, "random.random"),
        (lab.BadTimeWorkflow, "datetime.datetime.now"),
        (lab.BadFileWorkflow, "open"),
        (lab.BadHttpWorkflow, "urllib.request.urlopen"),
    ],
)
async def test_sandbox_blocks_the_obvious_mistakes(local_env, queue, bad, blocked_call):
    async with Worker(local_env.client, task_queue=queue, workflows=[bad]):
        handle = await local_env.client.start_workflow(bad.run, id=queue, task_queue=queue)
        message = None
        while message is None:
            message = await workflow_task_failure(handle)
            await asyncio.sleep(0.05)
        assert blocked_call in message
        assert (await handle.describe()).status.name == "RUNNING"
        await handle.terminate()


@pytest.mark.integration
async def test_blocking_the_event_loop_is_refused(local_env, queue):
    async with Worker(local_env.client, task_queue=queue, workflows=[lab.BadBlockingWorkflow]):
        handle = await local_env.client.start_workflow(lab.BadBlockingWorkflow.run, id=queue, task_queue=queue)
        message = None
        while message is None:
            message = await workflow_task_failure(handle)
            await asyncio.sleep(0.05)
        print("OBSERVED blocking:", message[:140])
        await handle.terminate()


@pytest.mark.integration
async def test_sandbox_does_not_catch_everything(local_env, queue):
    """A direct database call runs and even succeeds. The sandbox is a seatbelt, not a proof."""
    async with Worker(local_env.client, task_queue=queue, workflows=[lab.BadDatabaseWorkflow]):
        result = await local_env.client.execute_workflow(lab.BadDatabaseWorkflow.run, id=queue, task_queue=queue)
    assert result == 1


async def test_good_values_survive_replay(local_env, queue):
    async with Worker(local_env.client, task_queue=queue, workflows=[lab.GoodDeterministicWorkflow]):
        handle = await local_env.client.start_workflow(lab.GoodDeterministicWorkflow.run, id=queue, task_queue=queue)
        first = await handle.result()
    await Replayer(workflows=[lab.GoodDeterministicWorkflow]).replay_workflow(await handle.fetch_history())
    assert 0 <= first.random_value < 1
