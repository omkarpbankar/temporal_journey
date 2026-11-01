"""Phase 3: a Workflow that validates, waits, calls an Activity, waits again, and finishes."""

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.commerce import CommerceActivities
    from shared.models import ChargeRequest, CheckoutResult, Order, validate_order

TASK_QUEUE = "order-basics"


@workflow.defn
class OrderBasicsWorkflow:
    def __init__(self) -> None:
        self.stage = "received"
        self.warehouse_ready = False

    @workflow.signal
    def mark_warehouse_ready(self) -> None:
        self.warehouse_ready = True

    @workflow.query
    def current_stage(self) -> str:
        return self.stage

    @workflow.run
    async def run(self, order: Order) -> CheckoutResult:
        # Safe: pure logic with the same answer on every replay.
        errors = validate_order(order)
        if errors:
            return CheckoutResult(order.order_id, "rejected", reason="; ".join(errors))

        # Safe: a durable timer. The Service keeps the clock, so no process has to stay alive.
        self.stage = "fraud_hold"
        await asyncio.sleep(timedelta(hours=1).total_seconds())

        # Must be an Activity: it talks to the outside world.
        self.stage = "charging"
        charge_id = await workflow.execute_activity_method(
            CommerceActivities.charge_payment,
            ChargeRequest(order.order_id, order.amount_cents, f"{order.order_id}-charge"),
            start_to_close_timeout=timedelta(seconds=10),
        )

        # Safe: waits for a Signal, durably, and gives up after a day.
        self.stage = "waiting_for_warehouse"
        try:
            await workflow.wait_condition(lambda: self.warehouse_ready, timeout=timedelta(days=1))
        except asyncio.TimeoutError:
            self.stage = "warehouse_timeout"
            return CheckoutResult(order.order_id, "needs_review", charge_id, "warehouse timeout")

        self.stage = "completed"
        return CheckoutResult(order.order_id, "completed", charge_id)
