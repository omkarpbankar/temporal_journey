from concurrent.futures import ThreadPoolExecutor

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Worker

from activities.commerce import CommerceActivities, Fault
from activities.long_running import ExportRequest, export_rows
from shared.ledger import Ledger
from shared.models import ChargeRequest, CheckoutInput, Customer, EmailRequest, Order, ReserveRequest
from tests.helpers.history import event_names
from workflows.checkout import CheckoutWorkflow, ExportWorkflow

ORDER = Order("o-1", "c-1", "keyboard", 1, 5000)


async def run_checkout(env, queue, ledger, faults=None, order=ORDER, **options):
    activities = CommerceActivities(ledger, faults or {})
    async with Worker(
        env.client,
        task_queue=queue,
        workflows=[CheckoutWorkflow],
        activities=[activities.charge_payment, activities.reserve_inventory, activities.send_email],
        activity_executor=ThreadPoolExecutor(4),
    ):
        handle = await env.client.start_workflow(
            CheckoutWorkflow.run, CheckoutInput(order, **options), id=queue, task_queue=queue
        )
        return await handle.result(), handle


async def test_happy_path(time_env, queue, ledger):
    result, _ = await run_checkout(time_env, queue, ledger)
    assert result.status == "completed"
    assert result.steps == ["charged", "reserved", "emailed"]


async def test_fail_once_then_succeed(time_env, queue, ledger):
    result, handle = await run_checkout(time_env, queue, ledger, {"charge_payment": Fault("transient", 1)})
    assert result.status == "completed"
    assert ledger.count("charge") == 1
    # A failed attempt that gets retried writes no event, so history stays small.
    assert "ACTIVITY_TASK_FAILED" not in await event_names(handle)


async def test_fail_three_times_then_succeed(time_env, queue, ledger):
    result, _ = await run_checkout(time_env, queue, ledger, {"reserve_inventory": Fault("transient", 3)})
    assert result.status == "completed"


async def test_fail_more_than_max_attempts(time_env, queue, ledger):
    result, _ = await run_checkout(time_env, queue, ledger, {"send_email": Fault("transient", 99)}, max_attempts=3)
    assert result.status == "failed"
    assert result.steps == ["charged", "reserved"]


async def test_permanent_failure_is_not_retried(time_env, queue, ledger):
    result, _ = await run_checkout(time_env, queue, ledger, {"charge_payment": Fault("permanent", 99)})
    assert result.status == "failed"
    assert ledger.count("charge") == 0


async def test_card_declined_stops_the_flow(time_env, queue, ledger):
    result, _ = await run_checkout(time_env, queue, ledger, order=Order("o-2", "c-1", "keyboard", 1, 2_000_000))
    assert result.status == "failed"
    assert "Card declined" in result.reason


async def test_out_of_stock_leaves_the_customer_charged(time_env, queue, tmp_path):
    """The Phase 1 Part C bug is still here. Temporal cannot know a refund is needed."""
    ledger = Ledger(tmp_path / "empty.db", stock=0)
    result, _ = await run_checkout(time_env, queue, ledger)
    assert result.status == "failed"
    assert ledger.count("charge") == 1
    assert ledger.count("refund") == 0


async def test_hanging_activity_times_out_and_retries(time_env, queue, ledger):
    result, _ = await run_checkout(
        time_env, queue, ledger, {"charge_payment": Fault("hang", 1, seconds=1.5)}, start_to_close_seconds=1
    )
    assert result.status == "completed"


async def test_lost_response_with_idempotency_charges_once(time_env, queue, ledger):
    result, _ = await run_checkout(time_env, queue, ledger, {"charge_payment": Fault("lost_response", 1)})
    assert result.status == "completed"
    assert ledger.count("charge") == 1


async def test_lost_response_without_idempotency_charges_twice(time_env, queue, ledger):
    """Temporal retried exactly as promised. The double charge is the Activity's design."""
    result, _ = await run_checkout(
        time_env, queue, ledger, {"charge_payment": Fault("lost_response", 1)}, use_idempotency=False
    )
    assert result.status == "completed"
    assert ledger.count("charge") == 2


def test_create_customer_is_idempotent(ledger):
    activities = CommerceActivities(ledger)
    first = ActivityEnvironment().run(activities.create_customer, Customer("c-1", "a@b.c"))
    second = ActivityEnvironment().run(activities.create_customer, Customer("c-1", "a@b.c"))
    assert first == second
    assert ledger.count("customer") == 1


def test_charge_directly_without_a_worker(ledger):
    charge_id = ActivityEnvironment().run(CommerceActivities(ledger).charge_payment, ChargeRequest("o-1", 100, "k"))
    assert charge_id.startswith("charge_")


def test_decline_raises_non_retryable(ledger):
    with pytest.raises(ApplicationError) as error:
        ActivityEnvironment().run(CommerceActivities(ledger).charge_payment, ChargeRequest("o-1", 2_000_000))
    assert error.value.non_retryable and error.value.type == "PaymentDeclined"


def test_reserve_reduces_stock(ledger):
    ActivityEnvironment().run(CommerceActivities(ledger).reserve_inventory, ReserveRequest("o-1", "keyboard", 3, "k"))
    assert ledger.stock() == 97


def test_reserve_retry_does_not_reduce_stock_twice(ledger):
    activities = CommerceActivities(ledger)
    request = ReserveRequest("o-1", "keyboard", 3, "k")
    ActivityEnvironment().run(activities.reserve_inventory, request)
    ActivityEnvironment().run(activities.reserve_inventory, request)
    assert ledger.stock() == 97


def test_out_of_stock_is_non_retryable(tmp_path):
    activities = CommerceActivities(Ledger(tmp_path / "x.db", stock=1))
    with pytest.raises(ApplicationError) as error:
        ActivityEnvironment().run(activities.reserve_inventory, ReserveRequest("o-1", "k", 2))
    assert error.value.type == "OutOfStock"


def test_email_with_key_sends_once(ledger):
    activities = CommerceActivities(ledger)
    request = EmailRequest("o-1", "hi", "k")
    ActivityEnvironment().run(activities.send_email, request)
    ActivityEnvironment().run(activities.send_email, request)
    assert ledger.count("email") == 1


async def test_heartbeat_reports_progress():
    env = ActivityEnvironment()
    beats = []
    env.on_heartbeat = lambda *details: beats.append(details[0])
    result = await env.run(export_rows, ExportRequest(rows=3, seconds_per_row=0))
    assert beats == [1, 2, 3]
    assert result.rows_this_attempt == 3


@pytest.mark.integration
async def test_retry_resumes_from_last_heartbeat(local_env, queue):
    async with Worker(local_env.client, task_queue=queue, workflows=[ExportWorkflow], activities=[export_rows]):
        result = await local_env.client.execute_workflow(
            ExportWorkflow.run,
            ExportRequest(rows=20, seconds_per_row=0.02, fail_at_row=15),
            id=queue,
            task_queue=queue,
        )
    print("\nOBSERVED resumed", result)
    assert result.attempt == 2
    assert result.resumed_from > 0
    assert result.rows_this_attempt < 20


@pytest.mark.parametrize("name, kind", [("write_database", "db_row"), ("call_external_api", "api_call")])
def test_retried_writes_apply_once(ledger, name, kind):
    activities = CommerceActivities(ledger)
    ActivityEnvironment().run(getattr(activities, name), "o-1")
    ActivityEnvironment().run(getattr(activities, name), "o-1")
    assert ledger.count(kind) == 1
