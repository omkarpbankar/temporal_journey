from concurrent.futures import ThreadPoolExecutor

import pytest
from temporalio.client import WorkflowUpdateFailedError
from temporalio.worker import Worker

from activities.commerce import CommerceActivities
from shared.models import Order
from tests.helpers.history import event_names
from workflows.interactive_order import InteractiveOrderInput, InteractiveOrderWorkflow

INPUT = InteractiveOrderInput(Order("o-1", "c-1", "keyboard", 1, 5000))


def start_worker(env, queue, ledger):
    activities = CommerceActivities(ledger)
    return Worker(
        env.client,
        task_queue=queue,
        workflows=[InteractiveOrderWorkflow],
        activities=[activities.reserve_inventory, activities.send_email],
        activity_executor=ThreadPoolExecutor(4),
    )


async def start(env, queue):
    return await env.client.start_workflow(InteractiveOrderWorkflow.run, INPUT, id=queue, task_queue=queue)


async def test_payment_signal_drives_to_completion(time_env, queue, ledger):
    async with start_worker(time_env, queue, ledger):
        handle = await start(time_env, queue)
        await handle.signal(InteractiveOrderWorkflow.confirm_payment, "pay-1")
        result = await handle.result()
    assert result.state == "COMPLETED"
    assert result.payment_reference == "pay-1"


async def test_duplicate_payment_signal_changes_nothing(time_env, queue, ledger):
    async with start_worker(time_env, queue, ledger):
        handle = await start(time_env, queue)
        await handle.signal(InteractiveOrderWorkflow.confirm_payment, "pay-1")
        await handle.signal(InteractiveOrderWorkflow.confirm_payment, "pay-2")
        result = await handle.result()
    assert result.payment_reference == "pay-1"
    assert ledger.count("email") == 1


async def test_cancel_signal(time_env, queue, ledger):
    async with start_worker(time_env, queue, ledger):
        handle = await start(time_env, queue)
        await handle.signal(InteractiveOrderWorkflow.cancel, "changed my mind")
        result = await handle.result()
    assert result.state == "CANCELLED"
    assert ledger.count("email") == 0


async def test_payment_timeout(time_env, queue, ledger):
    async with start_worker(time_env, queue, ledger):
        handle = await start(time_env, queue)
        result = await handle.result()
    assert result.state == "PAYMENT_TIMED_OUT"


async def test_query_reads_state_and_writes_no_history(time_env, queue, ledger):
    async with start_worker(time_env, queue, ledger):
        handle = await start(time_env, queue)
        await handle.signal(InteractiveOrderWorkflow.cancel, "stop")
        await handle.result()
        before = await event_names(handle)
        status = await handle.query(InteractiveOrderWorkflow.status)
    assert status.state == "CANCELLED"
    assert await event_names(handle) == before


async def test_signal_with_start_never_misses_the_first_signal(time_env, queue, ledger):
    async with start_worker(time_env, queue, ledger):
        handle = await time_env.client.start_workflow(
            InteractiveOrderWorkflow.run,
            INPUT,
            id=queue,
            task_queue=queue,
            start_signal="confirm_payment",
            start_signal_args=["pay-early"],
        )
        result = await handle.result()
    assert result.payment_reference == "pay-early"


@pytest.mark.integration
async def test_update_returns_a_value_to_the_caller(local_env, queue, ledger):
    async with start_worker(local_env, queue, ledger):
        handle = await start(local_env, queue)
        previous = await handle.execute_update(InteractiveOrderWorkflow.change_address, "2 New Rd")
        assert previous == "1 Main St"
        await handle.signal(InteractiveOrderWorkflow.confirm_payment, "pay-1")
        result = await handle.result()
    assert result.address == "2 New Rd"


@pytest.mark.integration
async def test_validator_rejects_bad_update_and_history_stays_clean(local_env, queue, ledger):
    async with start_worker(local_env, queue, ledger):
        handle = await start(local_env, queue)
        with pytest.raises(WorkflowUpdateFailedError):
            await handle.execute_update(InteractiveOrderWorkflow.change_address, "   ")
        assert not [n for n in await event_names(handle) if "UPDATE" in n]
        await handle.signal(InteractiveOrderWorkflow.cancel, "done")
        await handle.result()


@pytest.mark.integration
async def test_update_rejected_once_too_late(local_env, queue, ledger):
    async with start_worker(local_env, queue, ledger):
        handle = await start(local_env, queue)
        await handle.signal(InteractiveOrderWorkflow.cancel, "done")
        await handle.result()
    # The run is closed now, so even asking is an error.
    with pytest.raises(Exception):
        await handle.execute_update(InteractiveOrderWorkflow.change_address, "2 New Rd")
