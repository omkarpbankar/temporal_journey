import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import CancelledError
from temporalio.worker import Worker

from activities.commerce import CommerceActivities, Fault
from shared.ledger import Ledger
from shared.models import Order
from workflows import composed_order as co

WORKFLOWS = [
    co.PaymentWorkflow,
    co.InventoryWorkflow,
    co.FulfillmentWorkflow,
    co.NotificationWorkflow,
    co.SequentialOrderWorkflow,
    co.ParallelOrderWorkflow,
    co.FanOutWorkflow,
]


@pytest.fixture
def order(queue) -> Order:
    # A unique ID per test. Child IDs come from it, and an abandoned child stays open.
    return Order(queue, "c-1", "keyboard", 1, 5000)


def start_worker(env, queue, ledger, faults=None):
    a = CommerceActivities(ledger, faults or {})
    return Worker(
        env.client,
        task_queue=queue,
        workflows=WORKFLOWS,
        activities=[
            a.charge_payment,
            a.refund_payment,
            a.reserve_inventory,
            a.release_inventory,
            a.call_external_api,
            a.send_email,
            a.write_database,
        ],
        activity_executor=ThreadPoolExecutor(8),
    )


async def test_sequential_children_all_run(time_env, queue, ledger, order):
    async with start_worker(time_env, queue, ledger):
        result = await time_env.client.execute_workflow(
            co.SequentialOrderWorkflow.run, order, id=queue, task_queue=queue
        )
    assert result.children == ["payment", "inventory", "fulfillment", "notification"]


async def test_child_failure_is_compensated_by_the_parent(time_env, queue, tmp_path, order):
    ledger = Ledger(tmp_path / "empty.db", stock=0)
    async with start_worker(time_env, queue, ledger):
        result = await time_env.client.execute_workflow(
            co.SequentialOrderWorkflow.run, order, id=queue, task_queue=queue
        )
    assert result.status == "refunded"
    assert (ledger.count("charge"), ledger.count("refund")) == (1, 1)


# Abandoned children run on local_env. On the time-skipping server, a run with one left
# that server unable to skip time for every later test, even after the child finished.
async def test_parallel_children_complete(local_env, queue, ledger, order):
    async with start_worker(local_env, queue, ledger):
        result = await local_env.client.execute_workflow(
            co.ParallelOrderWorkflow.run, order, id=queue, task_queue=queue
        )
        await local_env.client.get_workflow_handle(f"{queue}-notification").result()
    assert result.status == "completed"


async def test_parallel_inventory_failure_refunds(time_env, queue, tmp_path, order):
    ledger = Ledger(tmp_path / "empty.db", stock=0)
    async with start_worker(time_env, queue, ledger):
        result = await time_env.client.execute_workflow(
            co.ParallelOrderWorkflow.run, order, id=queue, task_queue=queue
        )
    assert result.status == "refunded"
    assert ledger.count("refund") == 1


async def test_parallel_payment_failure_releases_stock(time_env, queue, ledger, order):
    order.amount_cents = 2_000_000
    async with start_worker(time_env, queue, ledger):
        result = await time_env.client.execute_workflow(
            co.ParallelOrderWorkflow.run, order, id=queue, task_queue=queue
        )
    assert result.status == "released"
    assert ledger.stock() == 100


async def test_abandoned_child_outlives_its_parent(local_env, queue, ledger, order):
    async with start_worker(local_env, queue, ledger):
        await local_env.client.execute_workflow(co.ParallelOrderWorkflow.run, order, id=queue, task_queue=queue)
        child = local_env.client.get_workflow_handle(f"{queue}-notification")
        await child.result()
    assert ledger.count("email") == 1


async def test_fan_out_keeps_going_past_failures(time_env, queue, ledger):
    faults = {"write_database": Fault("permanent", 99)}
    async with start_worker(time_env, queue, ledger, faults):
        result = await time_env.client.execute_workflow(
            co.FanOutWorkflow.run, co.FanOutInput(["a", "b"]), id=queue, task_queue=queue
        )
    assert result == {"succeeded": [], "failed": ["a", "b"]}


async def test_fan_out_processes_every_item(time_env, queue, ledger):
    ids = [f"item-{n}" for n in range(10)]
    async with start_worker(time_env, queue, ledger):
        result = await time_env.client.execute_workflow(
            co.FanOutWorkflow.run, co.FanOutInput(ids, 2), id=queue, task_queue=queue
        )
    assert sorted(result["succeeded"]) == sorted(ids)
    assert ledger.count("db_row") == 10


@pytest.mark.integration
async def test_cancelling_the_parent_cancels_the_child(local_env, queue, ledger, order):
    # No Activity Worker for the child's step, so the child sits waiting.
    async with Worker(local_env.client, task_queue=queue, workflows=WORKFLOWS):
        handle = await local_env.client.start_workflow(
            co.SequentialOrderWorkflow.run, order, id=queue, task_queue=queue
        )
        child = local_env.client.get_workflow_handle(f"{queue}-payment")
        for _ in range(50):
            try:
                await child.describe()
                break
            except Exception:
                await asyncio.sleep(0.05)
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as error:
            await handle.result()
    assert isinstance(error.value.cause, CancelledError)
    assert (await child.describe()).status.name == "CANCELED"
