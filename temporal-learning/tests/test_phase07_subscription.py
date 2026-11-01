import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import CancelledError
from temporalio.worker import Worker

from activities.commerce import CommerceActivities
from tests.helpers.history import event_names
from workflows.subscription import SubscriptionInput, SubscriptionWorkflow


def start_worker(env, queue, ledger):
    activities = CommerceActivities(ledger)
    return Worker(
        env.client,
        task_queue=queue,
        workflows=[SubscriptionWorkflow],
        activities=[activities.charge_payment, activities.send_email],
        activity_executor=ThreadPoolExecutor(4),
    )


async def test_a_simulated_year_takes_seconds(time_env, queue, ledger):
    started = time.monotonic()
    async with start_worker(time_env, queue, ledger):
        result = await time_env.client.execute_workflow(
            SubscriptionWorkflow.run, SubscriptionInput("c-1", 999, total_periods=12), id=queue, task_queue=queue
        )
    print("\nOBSERVED 360 simulated days took", round(time.monotonic() - started, 2), "seconds")
    assert result.state == "COMPLETED"
    assert result.periods_paid == 12
    assert ledger.count("charge") == 12


async def test_continue_as_new_keeps_state_and_resets_history(time_env, queue, ledger):
    async with start_worker(time_env, queue, ledger):
        handle = await time_env.client.start_workflow(
            SubscriptionWorkflow.run,
            SubscriptionInput("c-1", 999, total_periods=12, periods_per_run=3),
            id=queue,
            task_queue=queue,
        )
        result = await handle.result(follow_runs=True)
        first_run = time_env.client.get_workflow_handle(queue, run_id=handle.first_execution_run_id)
        first_run_events = await event_names(first_run)
        last_run_events = await event_names(time_env.client.get_workflow_handle(queue))
    print("OBSERVED events first run", len(first_run_events), "last run", len(last_run_events))
    assert first_run_events[-1] == "WORKFLOW_EXECUTION_CONTINUED_AS_NEW"
    assert result.periods_paid == 12
    assert len(result.charge_ids) == 12
    assert len(last_run_events) < 40


async def test_cancel_signal_sends_goodbye(time_env, queue, ledger):
    async with start_worker(time_env, queue, ledger):
        handle = await time_env.client.start_workflow(
            SubscriptionWorkflow.run, SubscriptionInput("c-1", 999), id=queue, task_queue=queue
        )
        await handle.signal(SubscriptionWorkflow.request_cancel)
        result = await handle.result()
    assert result.state == "CANCELLED"
    assert ledger.count("email") == 1


async def test_declined_card_lapses_after_grace(time_env, queue, ledger):
    async with start_worker(time_env, queue, ledger):
        result = await time_env.client.execute_workflow(
            SubscriptionWorkflow.run, SubscriptionInput("c-1", 2_000_000), id=queue, task_queue=queue
        )
    assert result.state == "LAPSED"
    assert result.periods_paid == 0


async def test_cancelling_the_workflow_still_cleans_up(time_env, queue, ledger):
    async with start_worker(time_env, queue, ledger):
        handle = await time_env.client.start_workflow(
            SubscriptionWorkflow.run, SubscriptionInput("c-1", 999), id=queue, task_queue=queue
        )
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as error:
            await handle.result()
    assert isinstance(error.value.cause, CancelledError)
    assert ledger.count("email") == 1


async def test_billing_period_key_prevents_double_charge(time_env, queue, ledger):
    from temporalio.testing import ActivityEnvironment

    from shared.models import ChargeRequest

    activities = CommerceActivities(ledger)
    request = ChargeRequest("c-1-3", 999, "c-1-period-3")
    ActivityEnvironment().run(activities.charge_payment, request)
    ActivityEnvironment().run(activities.charge_payment, request)
    assert ledger.count("charge") == 1


@pytest.mark.integration
async def test_wait_for_a_query_while_subscribed(local_env, queue, ledger):
    async with start_worker(local_env, queue, ledger):
        handle = await local_env.client.start_workflow(
            SubscriptionWorkflow.run, SubscriptionInput("c-1", 999), id=queue, task_queue=queue
        )
        await asyncio.sleep(0.2)
        assert await handle.query(SubscriptionWorkflow.current_state) == "SUBSCRIBED"
        await handle.terminate()
