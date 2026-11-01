"""Phase 9: one Workflow, with each step sent to its own pool of Workers."""

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.commerce import CommerceActivities
    from activities.cpu_heavy import hash_block, slow_call
    from shared.models import ChargeRequest, EmailRequest, Order, ReserveRequest

ORDERS = "orders"
PAYMENTS = "payments"
INVENTORY = "inventory"
NOTIFICATIONS = "notifications"
DATA_PROCESSING = "data-processing"


@dataclass
class RoutedOrderInput:
    order: Order
    # Lets tests share one server without their queues colliding.
    queue_prefix: str = ""


@workflow.defn
class RoutedOrderWorkflow:
    def __init__(self) -> None:
        self.stage = "started"

    @workflow.query
    def current_stage(self) -> str:
        return self.stage

    @workflow.run
    async def run(self, input: RoutedOrderInput) -> str:
        order, prefix = input.order, input.queue_prefix
        timeout = timedelta(seconds=30)

        self.stage = "charging"
        await workflow.execute_activity_method(
            CommerceActivities.charge_payment,
            ChargeRequest(order.order_id, order.amount_cents, f"{order.order_id}-charge"),
            task_queue=prefix + PAYMENTS,
            start_to_close_timeout=timeout,
        )
        self.stage = "reserving"
        await workflow.execute_activity_method(
            CommerceActivities.reserve_inventory,
            ReserveRequest(order.order_id, order.item, order.quantity, f"{order.order_id}-reserve"),
            task_queue=prefix + INVENTORY,
            start_to_close_timeout=timeout,
        )
        self.stage = "notifying"
        await workflow.execute_activity_method(
            CommerceActivities.send_email,
            EmailRequest(order.order_id, "Your order is confirmed", f"{order.order_id}-email"),
            task_queue=prefix + NOTIFICATIONS,
            # Schedule-To-Close covers queue time too, so this bounds a Notification Worker outage.
            schedule_to_close_timeout=timedelta(hours=1),
        )
        self.stage = "completed"
        return "completed"


@workflow.defn
class BurstWorkflow:
    """Schedules many Activities at once, to measure a queue's real capacity."""

    @workflow.run
    async def run(self, count: int) -> int:
        await asyncio.gather(
            *(
                workflow.execute_activity(slow_call, 0.3, start_to_close_timeout=timedelta(seconds=30))
                for _ in range(count)
            )
        )
        return count


@dataclass
class CpuJob:
    rounds: int
    queue_prefix: str = ""


@workflow.defn
class CpuJobWorkflow:
    @workflow.run
    async def run(self, job: CpuJob) -> str:
        return await workflow.execute_activity(
            hash_block,
            job.rounds,
            task_queue=job.queue_prefix + DATA_PROCESSING,
            start_to_close_timeout=timedelta(minutes=5),
        )
