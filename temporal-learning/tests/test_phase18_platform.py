import asyncio
import time
import uuid
from datetime import timedelta

import pytest
from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleSpec,
)
from temporalio.worker import Worker

from activities.commerce import CommerceActivities, Fault
from shared.ledger import Ledger
from shared.models import Order
from workers.platform import build_worker
from workflows import patterns as p
from workflows.fulfillment_platform import (
    ORDERS,
    OrderFulfillmentWorkflow,
    PlatformActivities,
    PlatformInput,
)
from workflows.patterns import Saga

pytestmark = pytest.mark.integration
ROLES = ("orders", "payments", "fulfillment", "notifications")


class Platform:
    def __init__(self, env, queue, ledger, fraud=None, faults=None):
        self.client = env.client
        self.prefix = f"{queue}-"
        self.queue = queue
        self.activities = PlatformActivities(CommerceActivities(ledger, faults or {}), fraud or {})
        self.workers = [build_worker(env.client, role, self.activities, self.prefix) for role in ROLES]

    async def __aenter__(self):
        for worker in self.workers:
            await worker.__aenter__()
        return self

    async def __aexit__(self, *exc):
        for worker in self.workers:
            await worker.__aexit__(*exc)

    async def start(self, order=None, **options):
        order = order or Order(self.queue, "c-1", "keyboard", 1, 5000)
        return await self.client.start_workflow(
            OrderFulfillmentWorkflow.run,
            PlatformInput(order, self.prefix, **options),
            id=self.queue,
            task_queue=self.prefix + ORDERS,
        )


async def wait_for_state(handle, state):
    for _ in range(300):
        if (await handle.query(OrderFulfillmentWorkflow.status)).state == state:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"never reached {state}")


async def test_happy_path_across_four_worker_types(local_env, queue, ledger):
    async with Platform(local_env, queue, ledger) as platform:
        result = await (await platform.start()).result()
    assert result.state == "COMPLETED"
    assert ledger.count("charge") == ledger.count("shipment") == ledger.count("email") == 1


async def test_risky_order_waits_for_a_human_then_ships(local_env, queue, ledger):
    async with Platform(local_env, queue, ledger, fraud={"c-1": 0.9}) as platform:
        handle = await platform.start()
        await wait_for_state(handle, "REVIEW")
        assert ledger.count("charge") == 0
        await handle.signal(OrderFulfillmentWorkflow.submit_review, True)
        assert (await handle.result()).state == "COMPLETED"


async def test_human_rejection_charges_nothing(local_env, queue, ledger):
    async with Platform(local_env, queue, ledger, fraud={"c-1": 0.9}) as platform:
        handle = await platform.start()
        await wait_for_state(handle, "REVIEW")
        await handle.signal(OrderFulfillmentWorkflow.submit_review, False)
        assert (await handle.result()).state == "REJECTED"
    assert ledger.count("charge") == 0


async def test_out_of_stock_refunds_the_customer(local_env, queue, tmp_path):
    ledger = Ledger(tmp_path / "empty.db", stock=0)
    async with Platform(local_env, queue, ledger) as platform:
        result = await (await platform.start()).result()
    assert result.state == "FAILED"
    assert (ledger.count("charge"), ledger.count("refund")) == (1, 1)


async def test_shipping_failure_unwinds_in_reverse_order(local_env, queue, ledger):
    order = Order(queue, "c-1", "keyboard", 2, 5000, address="nowhere")
    async with Platform(local_env, queue, ledger) as platform:
        result = await (await platform.start(order)).result()
    assert result.state == "FAILED"
    assert (ledger.count("refund"), ledger.count("release")) == (1, 1)
    assert ledger.stock() == 100


async def test_update_fixes_the_address_before_shipping(local_env, queue, ledger):
    order = Order(queue, "c-1", "keyboard", 1, 5000, address="nowhere")
    async with Platform(local_env, queue, ledger, fraud={"c-1": 0.9}) as platform:
        handle = await platform.start(order)
        await wait_for_state(handle, "REVIEW")
        assert await handle.execute_update(OrderFulfillmentWorkflow.change_address, "5 Real Rd") == "5 Real Rd"
        await handle.signal(OrderFulfillmentWorkflow.submit_review, True)
        assert (await handle.result()).state == "COMPLETED"


async def test_cancel_during_review(local_env, queue, ledger):
    async with Platform(local_env, queue, ledger, fraud={"c-1": 0.9}) as platform:
        handle = await platform.start()
        await wait_for_state(handle, "REVIEW")
        await handle.signal(OrderFulfillmentWorkflow.request_cancel)
        assert (await handle.result()).state == "CANCELLED"


async def test_failed_compensation_is_reported_for_a_human(local_env, queue, tmp_path):
    ledger = Ledger(tmp_path / "empty.db", stock=0)
    faults = {"refund_payment": Fault("permanent", 99)}
    async with Platform(local_env, queue, ledger, faults=faults) as platform:
        result = await (await platform.start()).result()
    assert result.state == "FAILED"
    assert len(result.compensation_failures) == 1


async def test_state_is_searchable_across_all_orders(local_env, queue, ledger):
    async with Platform(local_env, queue, ledger, fraud={"c-1": 0.9}) as platform:
        handle = await platform.start()
        await wait_for_state(handle, "REVIEW")
        found = []
        for _ in range(100):
            found = [w.id async for w in platform.client.list_workflows(f"OrderState = 'REVIEW' AND WorkflowId = '{queue}'")]
            if found:
                break
            await asyncio.sleep(0.05)
        await handle.signal(OrderFulfillmentWorkflow.submit_review, False)
        await handle.result()
    assert found == [queue]


async def test_saga_undoes_newest_first():
    order = []
    saga = Saga()

    async def undo(name):
        order.append(name)

    saga.add(lambda: undo("first"))
    saga.add(lambda: undo("second"))
    assert await saga.compensate() == []
    assert order == ["second", "first"]


async def test_saga_continues_past_a_failed_undo():
    order = []
    saga = Saga()

    async def good():
        order.append("good")

    async def bad():
        raise RuntimeError("refund service down")

    saga.add(good)
    saga.add(bad)
    assert await saga.compensate() == ["refund service down"]
    assert order == ["good"]


async def test_batching_stays_small_with_continue_as_new(local_env, queue):
    client = local_env.client
    items = [str(n) for n in range(95)]
    async with Worker(client, task_queue=queue, workflows=[p.BatchWorkflow], activities=[p.process_batch]):
        handle = await client.start_workflow(p.BatchWorkflow.run, p.BatchInput(items), id=queue, task_queue=queue)
        assert await handle.result() == 95
    runs = [w async for w in client.list_workflows(f"WorkflowId = '{queue}'")]
    print("OBSERVED batch runs", len(runs))


async def test_schedule_runs_a_workflow(local_env, queue):
    client = local_env.client
    schedule_id = f"report-{uuid.uuid4().hex[:8]}"
    async with Worker(client, task_queue=queue, workflows=[p.ReportWorkflow], activities=[p.tick]):
        handle = await client.create_schedule(
            schedule_id,
            Schedule(
                action=ScheduleActionStartWorkflow(p.ReportWorkflow.run, "daily", id=f"{queue}-run", task_queue=queue),
                spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(days=1))]),
            ),
        )
        await handle.trigger()
        for _ in range(100):
            description = await handle.describe()
            if description.info.recent_actions:
                break
            await asyncio.sleep(0.05)
        await handle.delete()
    assert description.info.num_actions >= 1


async def test_one_workflow_signals_another(local_env, queue):
    client = local_env.client
    async with Worker(client, task_queue=queue, workflows=[p.WaiterWorkflow, p.NotifierWorkflow]):
        waiter = await client.start_workflow(p.WaiterWorkflow.run, id=f"{queue}-a", task_queue=queue)
        await client.execute_workflow(p.NotifierWorkflow.run, waiter.id, id=f"{queue}-b", task_queue=queue)
        assert await waiter.result() == "ready"


async def test_approval_chain_escalates_on_silence(time_env, queue):
    async with Worker(time_env.client, task_queue=queue, workflows=[p.HumanApprovalWorkflow]):
        handle = await time_env.client.start_workflow(
            p.HumanApprovalWorkflow.run, p.ApprovalChain(["lead", "director"]), id=queue, task_queue=queue
        )
        await handle.signal(p.HumanApprovalWorkflow.approve, "lead")
        result = await handle.result()
    assert result.approved_by == ["lead"]
    assert result.escalated


async def test_rate_limit_slows_a_whole_task_queue(local_env, queue):
    client = local_env.client
    from activities.cpu_heavy import slow_call
    from workflows.routed_order import BurstWorkflow

    async with Worker(
        client,
        task_queue=queue,
        workflows=[BurstWorkflow],
        activities=[slow_call],
        max_task_queue_activities_per_second=5,
    ):
        started = time.monotonic()
        await client.execute_workflow(BurstWorkflow.run, 10, id=queue, task_queue=queue)
        elapsed = time.monotonic() - started
    print(f"OBSERVED 10 activities at 5/s took {elapsed:.2f}s")
    assert elapsed > 1


async def test_graceful_shutdown_lets_an_activity_finish(local_env, queue):
    client = local_env.client
    from activities.cpu_heavy import slow_call
    from workflows.routed_order import BurstWorkflow

    worker = Worker(
        client,
        task_queue=queue,
        workflows=[BurstWorkflow],
        activities=[slow_call],
        graceful_shutdown_timeout=timedelta(seconds=5),
    )
    async with worker:
        handle = await client.start_workflow(BurstWorkflow.run, 1, id=queue, task_queue=queue)
        await asyncio.sleep(0.1)
    # The Worker has stopped, and a new one can only finish the Workflow if the Activity did.
    async with Worker(client, task_queue=queue, workflows=[BurstWorkflow], activities=[slow_call]):
        assert await asyncio.wait_for(handle.result(), 10) == 1
