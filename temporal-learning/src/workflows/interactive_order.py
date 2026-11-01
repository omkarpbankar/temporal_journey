"""Phase 6: a Workflow that the outside world talks to while it runs."""

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.commerce import CommerceActivities
    from shared.models import EmailRequest, Order, ReserveRequest

TASK_QUEUE = "interactive-orders"


@dataclass
class InteractiveOrderInput:
    order: Order
    payment_timeout_hours: float = 24


@dataclass
class OrderStatus:
    state: str
    address: str
    payment_reference: str | None
    cancel_reason: str | None


@workflow.defn
class InteractiveOrderWorkflow:
    @workflow.init
    def __init__(self, input: InteractiveOrderInput) -> None:
        # workflow.init gets the input before any handler can run, so a Signal or
        # Query that arrives first still sees a fully built object.
        self.state = "ORDER_CREATED"
        self.address = input.order.address
        self.payment_reference: str | None = None
        self.cancel_reason: str | None = None

    @workflow.signal
    def confirm_payment(self, reference: str) -> None:
        # Signals can be delivered more than once, so a repeat must change nothing.
        if self.payment_reference is None:
            self.payment_reference = reference

    @workflow.signal
    def cancel(self, reason: str) -> None:
        if self.payment_reference is None:
            self.cancel_reason = reason

    @workflow.query
    def status(self) -> OrderStatus:
        return OrderStatus(self.state, self.address, self.payment_reference, self.cancel_reason)

    @workflow.update
    def change_address(self, address: str) -> str:
        previous, self.address = self.address, address
        return previous

    @change_address.validator
    def validate_address(self, address: str) -> None:
        # A rejected Update leaves no trace in history, and the caller gets the error.
        if not address.strip():
            raise ValueError("address cannot be empty")
        if self.state in ("FULFILLING", "COMPLETED", "CANCELLED", "PAYMENT_TIMED_OUT"):
            raise ValueError(f"too late to change the address: order is {self.state}")

    @workflow.run
    async def run(self, input: InteractiveOrderInput) -> OrderStatus:
        order = input.order
        self.state = "WAITING_FOR_PAYMENT"
        try:
            await workflow.wait_condition(
                lambda: self.payment_reference is not None or self.cancel_reason is not None,
                timeout=timedelta(hours=input.payment_timeout_hours),
            )
        except asyncio.TimeoutError:
            self.state = "PAYMENT_TIMED_OUT"
            return self.status()

        if self.cancel_reason is not None:
            self.state = "CANCELLED"
            return self.status()

        self.state = "PAYMENT_CONFIRMED"
        await workflow.wait_condition(workflow.all_handlers_finished)

        self.state = "FULFILLING"
        await workflow.execute_activity_method(
            CommerceActivities.reserve_inventory,
            ReserveRequest(order.order_id, order.item, order.quantity, f"{order.order_id}-reserve"),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity_method(
            CommerceActivities.send_email,
            EmailRequest(order.order_id, f"Shipping to {self.address}", f"{order.order_id}-email"),
            start_to_close_timeout=timedelta(seconds=30),
        )
        self.state = "COMPLETED"
        return self.status()
