"""Phase 7: a Workflow that lives for years, one month at a time."""

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, is_cancelled_exception

with workflow.unsafe.imports_passed_through():
    from activities.commerce import CommerceActivities
    from shared.models import ChargeRequest, EmailRequest

TASK_QUEUE = "subscriptions"
RENEWAL_DAYS = 30
GRACE_DAYS = 7
NO_RETRY = RetryPolicy(maximum_attempts=1)


@dataclass
class SubscriptionInput:
    customer_id: str
    amount_cents: int
    total_periods: int = 12
    periods_per_run: int = 12
    renewal_days: float = RENEWAL_DAYS
    # Everything below is carried across Continue-As-New.
    periods_paid: int = 0
    charge_ids: list[str] = field(default_factory=list)


@dataclass
class SubscriptionResult:
    state: str
    periods_paid: int
    charge_ids: list[str]


@workflow.defn
class SubscriptionWorkflow:
    @workflow.init
    def __init__(self, input: SubscriptionInput) -> None:
        self.state = "SUBSCRIBED"
        self.periods_paid = input.periods_paid
        self.charge_ids = list(input.charge_ids)
        self.cancel_requested = False
        self.payment_fixed = False

    @workflow.signal
    def request_cancel(self) -> None:
        self.cancel_requested = True

    @workflow.signal
    def payment_method_updated(self) -> None:
        self.payment_fixed = True

    @workflow.query
    def current_state(self) -> str:
        return self.state

    @workflow.query
    def paid(self) -> int:
        return self.periods_paid

    def _result(self) -> SubscriptionResult:
        return SubscriptionResult(self.state, self.periods_paid, self.charge_ids)

    async def _charge(self, input: SubscriptionInput) -> str:
        period = self.periods_paid + 1
        return await workflow.execute_activity_method(
            CommerceActivities.charge_payment,
            # One key per billing period: a retry of March never becomes a second March.
            ChargeRequest(f"{input.customer_id}-{period}", input.amount_cents, f"{input.customer_id}-period-{period}"),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3, non_retryable_error_types=["PaymentDeclined"]),
        )

    async def _say_goodbye(self, input: SubscriptionInput) -> None:
        await workflow.execute_activity_method(
            CommerceActivities.send_email,
            EmailRequest(input.customer_id, "Sorry to see you go", f"{input.customer_id}-goodbye"),
            start_to_close_timeout=timedelta(seconds=30),
        )

    @workflow.run
    async def run(self, input: SubscriptionInput) -> SubscriptionResult:
        periods_this_run = 0
        try:
            while self.periods_paid < input.total_periods:
                self.state = "SUBSCRIBED"
                try:
                    await workflow.wait_condition(
                        lambda: self.cancel_requested, timeout=timedelta(days=input.renewal_days)
                    )
                    self.state = "CANCELLED"
                    await self._say_goodbye(input)
                    return self._result()
                except asyncio.TimeoutError:
                    pass  # A month has passed, so it's time to renew.

                self.state = "RENEWAL_DUE"
                try:
                    self.charge_ids.append(await self._charge(input))
                except ActivityError as error:
                    # A cancel that lands mid-charge also arrives here. It is not a decline.
                    if is_cancelled_exception(error):
                        raise
                    self.state = "PAYMENT_FAILED"
                    self.payment_fixed = False
                    try:
                        await workflow.wait_condition(
                            lambda: self.payment_fixed or self.cancel_requested,
                            timeout=timedelta(days=GRACE_DAYS),
                        )
                    except asyncio.TimeoutError:
                        self.state = "LAPSED"
                        return self._result()
                    if self.cancel_requested:
                        self.state = "CANCELLED"
                        await self._say_goodbye(input)
                        return self._result()
                    continue

                self.periods_paid += 1
                periods_this_run += 1

                should_roll = periods_this_run >= input.periods_per_run
                if self.periods_paid < input.total_periods and (
                    should_roll or workflow.info().is_continue_as_new_suggested()
                ):
                    # Never roll over while a handler is mid-flight, or its work is lost.
                    await workflow.wait_condition(workflow.all_handlers_finished)
                    workflow.continue_as_new(
                        SubscriptionInput(
                            customer_id=input.customer_id,
                            amount_cents=input.amount_cents,
                            total_periods=input.total_periods,
                            periods_per_run=input.periods_per_run,
                            renewal_days=input.renewal_days,
                            periods_paid=self.periods_paid,
                            charge_ids=self.charge_ids,
                        )
                    )
        except (asyncio.CancelledError, ActivityError) as error:
            # A cancel arrives as CancelledError while waiting, but as ActivityError
            # while an Activity runs. is_cancelled_exception recognises both.
            if not is_cancelled_exception(error):
                raise
            # Activities can still run here, so clean up, then re-raise so the
            # Workflow is recorded as Cancelled.
            self.state = "CANCELLED"
            await self._say_goodbye(input)
            raise

        self.state = "COMPLETED"
        return self._result()
