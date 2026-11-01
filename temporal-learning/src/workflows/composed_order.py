"""Phase 8: a large process built from smaller durable pieces."""

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ChildWorkflowError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from activities.commerce import CommerceActivities
    from shared.models import ChargeRequest, EmailRequest, Order, ReserveRequest

TASK_QUEUE = "composed-orders"
STEP = dict(
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=RetryPolicy(maximum_attempts=3, initial_interval=timedelta(milliseconds=100)),
)


@dataclass
class ComposedResult:
    status: str
    children: list[str] = field(default_factory=list)
    reason: str | None = None


@workflow.defn
class PaymentWorkflow:
    @workflow.run
    async def run(self, order: Order) -> str:
        return await workflow.execute_activity_method(
            CommerceActivities.charge_payment,
            ChargeRequest(order.order_id, order.amount_cents, f"{order.order_id}-charge"),
            **STEP,
        )


@workflow.defn
class InventoryWorkflow:
    @workflow.run
    async def run(self, order: Order) -> str:
        return await workflow.execute_activity_method(
            CommerceActivities.reserve_inventory,
            ReserveRequest(order.order_id, order.item, order.quantity, f"{order.order_id}-reserve"),
            **STEP,
        )


@workflow.defn
class FulfillmentWorkflow:
    @workflow.run
    async def run(self, order: Order) -> str:
        return await workflow.execute_activity_method(
            CommerceActivities.call_external_api, order.order_id, **STEP
        )


@workflow.defn
class NotificationWorkflow:
    @workflow.run
    async def run(self, order: Order) -> str:
        return await workflow.execute_activity_method(
            CommerceActivities.send_email,
            EmailRequest(order.order_id, "Order complete", f"{order.order_id}-email"),
            **STEP,
        )


def child_id(step: str, order: Order) -> str:
    # Stable and readable: you can find this child in the UI from the order alone.
    return f"{order.order_id}-{step}"


async def refund(order: Order) -> None:
    await workflow.execute_activity_method(
        CommerceActivities.refund_payment,
        ChargeRequest(order.order_id, order.amount_cents, f"{order.order_id}-refund"),
        **STEP,
    )


@workflow.defn
class SequentialOrderWorkflow:
    @workflow.run
    async def run(self, order: Order) -> ComposedResult:
        done = []
        await workflow.execute_child_workflow(PaymentWorkflow.run, order, id=child_id("payment", order))
        done.append("payment")
        try:
            await workflow.execute_child_workflow(InventoryWorkflow.run, order, id=child_id("inventory", order))
        except ChildWorkflowError as error:
            # The parent decides what a child's failure means for the business.
            await refund(order)
            return ComposedResult("refunded", done, str(error.cause.cause))
        done.append("inventory")
        await workflow.execute_child_workflow(FulfillmentWorkflow.run, order, id=child_id("fulfillment", order))
        done.append("fulfillment")
        await workflow.execute_child_workflow(NotificationWorkflow.run, order, id=child_id("notification", order))
        done.append("notification")
        return ComposedResult("completed", done)


@workflow.defn
class ParallelOrderWorkflow:
    """Payment and inventory don't depend on each other, so run them together."""

    @workflow.run
    async def run(self, order: Order) -> ComposedResult:
        payment, inventory = await asyncio.gather(
            workflow.execute_child_workflow(PaymentWorkflow.run, order, id=child_id("payment", order)),
            workflow.execute_child_workflow(InventoryWorkflow.run, order, id=child_id("inventory", order)),
            # Without this, the first failure raises while the other child keeps running.
            return_exceptions=True,
        )
        if isinstance(inventory, BaseException):
            if not isinstance(payment, BaseException):
                await refund(order)
            return ComposedResult("refunded", ["payment"], str(inventory))
        if isinstance(payment, BaseException):
            await workflow.execute_activity_method(
                CommerceActivities.release_inventory,
                ReserveRequest(order.order_id, order.item, order.quantity, f"{order.order_id}-release"),
                **STEP,
            )
            return ComposedResult("released", ["inventory"], str(payment))

        await workflow.execute_child_workflow(FulfillmentWorkflow.run, order, id=child_id("fulfillment", order))
        # Fire and forget: the email outlives this parent, which returns right away.
        await workflow.start_child_workflow(
            NotificationWorkflow.run,
            order,
            id=child_id("notification", order),
            parent_close_policy=ParentClosePolicy.ABANDON,
        )
        return ComposedResult("completed", ["payment", "inventory", "fulfillment", "notification"])


@dataclass
class FanOutInput:
    order_ids: list[str]
    max_concurrent: int = 3


@workflow.defn
class FanOutWorkflow:
    """Process many items at once, with a limit, and keep going when some fail."""

    @workflow.run
    async def run(self, input: FanOutInput) -> dict[str, list[str]]:
        limit = asyncio.Semaphore(input.max_concurrent)

        async def handle(order_id: str) -> tuple[str, bool]:
            async with limit:
                try:
                    await workflow.execute_activity_method(
                        CommerceActivities.write_database, order_id, **STEP
                    )
                    return order_id, True
                except ActivityError:
                    return order_id, False

        outcomes = await asyncio.gather(*(handle(order_id) for order_id in input.order_ids))
        return {
            "succeeded": [order_id for order_id, ok in outcomes if ok],
            "failed": [order_id for order_id, ok in outcomes if not ok],
        }
