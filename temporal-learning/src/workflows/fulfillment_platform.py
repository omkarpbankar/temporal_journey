"""Phase 18 project: the Order Fulfillment Platform.

Almost everything from Phases 3 to 17 appears here. Read run() first: it reads like the
business process, and the machinery stays out of the way.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy, SearchAttributeKey
from temporalio.exceptions import ActivityError, ApplicationError, ChildWorkflowError, is_cancelled_exception

with workflow.unsafe.imports_passed_through():
    from activities.commerce import CommerceActivities
    from shared.models import ChargeRequest, EmailRequest, Order, ReserveRequest
    from workflows.patterns import Saga

ORDERS = "platform-orders"
PAYMENTS = "platform-payments"
FULFILLMENT = "platform-fulfillment"
NOTIFICATIONS = "platform-notifications"
ORDER_STATE = SearchAttributeKey.for_keyword("OrderState")

RETRY = RetryPolicy(
    initial_interval=timedelta(milliseconds=100),
    maximum_interval=timedelta(seconds=5),
    maximum_attempts=5,
    non_retryable_error_types=["PaymentDeclined", "OutOfStock", "AddressInvalid"],
)
STEP = dict(start_to_close_timeout=timedelta(seconds=30), retry_policy=RETRY)


@dataclass
class ShipRequest:
    order_id: str
    address: str


@dataclass
class PlatformActivities:
    commerce: CommerceActivities
    fraud_scores: dict[str, float] = field(default_factory=dict)

    @activity.defn
    def fraud_check(self, order: Order) -> float:
        return self.fraud_scores.get(order.customer_id, 0.1)

    @activity.defn
    def ship(self, request: ShipRequest) -> str:
        if not request.address.strip() or "nowhere" in request.address:
            raise ApplicationError("cannot deliver there", type="AddressInvalid", non_retryable=True)
        return self.commerce.ledger.apply("shipment", request.order_id, request.address, f"{request.order_id}-ship")


@dataclass
class PlatformInput:
    order: Order
    prefix: str = ""
    review_hours: float = 24


@dataclass
class PlatformStatus:
    state: str
    address: str
    fraud_score: float | None = None


@dataclass
class PlatformResult:
    state: str
    reason: str | None = None
    compensation_failures: list[str] = field(default_factory=list)


@workflow.defn
class PaymentChildWorkflow:
    @workflow.run
    async def run(self, input: PlatformInput) -> str:
        order = input.order
        return await workflow.execute_activity_method(
            CommerceActivities.charge_payment,
            ChargeRequest(order.order_id, order.amount_cents, f"{order.order_id}-charge"),
            task_queue=input.prefix + PAYMENTS,
            **STEP,
        )


@workflow.defn
class OrderFulfillmentWorkflow:
    @workflow.init
    def __init__(self, input: PlatformInput) -> None:
        self.state = "RECEIVED"
        self.address = input.order.address
        self.fraud_score: float | None = None
        self.review_decision: bool | None = None
        self.cancel_requested = False

    @workflow.signal
    def submit_review(self, approved: bool) -> None:
        self.review_decision = approved

    @workflow.signal
    def request_cancel(self) -> None:
        self.cancel_requested = True

    @workflow.query
    def status(self) -> PlatformStatus:
        return PlatformStatus(self.state, self.address, self.fraud_score)

    @workflow.update
    def change_address(self, address: str) -> str:
        self.address = address
        return self.address

    @change_address.validator
    def check_address(self, address: str) -> None:
        if self.state in ("SHIPPING", "COMPLETED", "CANCELLED", "REJECTED", "FAILED"):
            raise ValueError(f"cannot change the address while {self.state}")
        if not address.strip():
            raise ValueError("address cannot be empty")

    def _enter(self, state: str) -> None:
        self.state = state
        # Search Attributes make "all orders stuck in REVIEW" a query rather than a script.
        workflow.upsert_search_attributes([ORDER_STATE.value_set(state)])

    async def _notify(self, input: PlatformInput, message: str, step: str) -> None:
        await workflow.execute_activity_method(
            CommerceActivities.send_email,
            EmailRequest(input.order.order_id, message, f"{input.order.order_id}-{step}"),
            task_queue=input.prefix + NOTIFICATIONS,
            **STEP,
        )

    async def _review(self, input: PlatformInput) -> bool:
        self._enter("REVIEW")
        try:
            await workflow.wait_condition(
                lambda: self.review_decision is not None or self.cancel_requested,
                timeout=timedelta(hours=input.review_hours),
            )
        except asyncio.TimeoutError:
            return False
        return bool(self.review_decision) and not self.cancel_requested

    async def _fulfill(self, input: PlatformInput, saga: Saga) -> None:
        order, prefix = input.order, input.prefix

        self._enter("PAYING")
        await workflow.execute_child_workflow(
            PaymentChildWorkflow.run,
            input,
            id=f"{order.order_id}-payment",
            task_queue=prefix + ORDERS,
        )

        async def refund() -> None:
            await workflow.execute_activity_method(
                CommerceActivities.refund_payment,
                ChargeRequest(order.order_id, order.amount_cents, f"{order.order_id}-refund"),
                task_queue=prefix + PAYMENTS,
                **STEP,
            )

        saga.add(refund)

        self._enter("RESERVING")
        await workflow.execute_activity_method(
            CommerceActivities.reserve_inventory,
            ReserveRequest(order.order_id, order.item, order.quantity, f"{order.order_id}-reserve"),
            task_queue=prefix + FULFILLMENT,
            **STEP,
        )

        async def release() -> None:
            await workflow.execute_activity_method(
                CommerceActivities.release_inventory,
                ReserveRequest(order.order_id, order.item, order.quantity, f"{order.order_id}-release"),
                task_queue=prefix + FULFILLMENT,
                **STEP,
            )

        saga.add(release)

        # Give a late cancel or address change a last chance, then wait for handlers to finish.
        await workflow.wait_condition(workflow.all_handlers_finished)
        if self.cancel_requested:
            raise ApplicationError("cancelled by customer", type="CustomerCancelled", non_retryable=True)

        self._enter("SHIPPING")
        await workflow.execute_activity_method(
            PlatformActivities.ship,
            ShipRequest(order.order_id, self.address),
            task_queue=prefix + FULFILLMENT,
            **STEP,
        )

    @workflow.run
    async def run(self, input: PlatformInput) -> PlatformResult:
        order, prefix = input.order, input.prefix
        self._enter("FRAUD_CHECK")
        self.fraud_score = await workflow.execute_activity_method(
            PlatformActivities.fraud_check, order, task_queue=prefix + PAYMENTS, **STEP
        )

        if self.fraud_score >= 0.7 and not await self._review(input):
            state = "CANCELLED" if self.cancel_requested else "REJECTED"
            self._enter(state)
            await self._notify(input, "We could not process your order", "rejected")
            return PlatformResult(state)

        saga = Saga()
        try:
            await self._fulfill(input, saga)
        except (asyncio.CancelledError, ActivityError, ChildWorkflowError, ApplicationError) as error:
            self._enter("COMPENSATING")
            if is_cancelled_exception(error):
                await asyncio.shield(saga.compensate())
                self._enter("CANCELLED")
                raise
            failures = await saga.compensate()
            state = "CANCELLED" if self.cancel_requested else "FAILED"
            self._enter(state)
            await self._notify(input, "Your order could not be completed", "failed")
            return PlatformResult(state, str(error.cause or error), failures)

        self._enter("COMPLETED")
        workflow.metric_meter().create_counter("orders_completed", "Orders fully shipped").add(1)
        await self._notify(input, f"Shipped to {self.address}", "shipped")
        return PlatformResult("COMPLETED")
