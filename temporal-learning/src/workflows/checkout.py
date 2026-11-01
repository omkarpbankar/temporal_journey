"""Phase 4: the Phase 1 checkout, rebuilt on Temporal."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from activities.commerce import CommerceActivities
    from activities.long_running import ExportRequest, ExportResult, export_rows
    from shared.models import (
        ChargeRequest,
        CheckoutInput,
        CheckoutResult,
        EmailRequest,
        ReserveRequest,
    )

TASK_QUEUE = "checkout"


@workflow.defn
class CheckoutWorkflow:
    @workflow.run
    async def run(self, input: CheckoutInput) -> CheckoutResult:
        order = input.order

        def key(step: str) -> str | None:
            # Stable across retries, so a repeat of this step is recognised as a repeat.
            return f"{order.order_id}-{step}" if input.use_idempotency else None

        options = dict(
            start_to_close_timeout=timedelta(seconds=input.start_to_close_seconds),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=100),
                maximum_interval=timedelta(seconds=1),
                maximum_attempts=input.max_attempts,
                non_retryable_error_types=["PaymentDeclined", "OutOfStock"],
            ),
        )

        steps = []
        try:
            charge_id = await workflow.execute_activity_method(
                CommerceActivities.charge_payment,
                ChargeRequest(order.order_id, order.amount_cents, key("charge")),
                **options,
            )
            steps.append("charged")
            await workflow.execute_activity_method(
                CommerceActivities.reserve_inventory,
                ReserveRequest(order.order_id, order.item, order.quantity, key("reserve")),
                **options,
            )
            steps.append("reserved")
            await workflow.execute_activity_method(
                CommerceActivities.send_email,
                EmailRequest(order.order_id, f"Receipt {charge_id}", key("email")),
                **options,
            )
            steps.append("emailed")
        except ActivityError as error:
            return CheckoutResult(order.order_id, "failed", reason=str(error.cause), steps=steps)
        return CheckoutResult(order.order_id, "completed", charge_id, steps=steps)


@workflow.defn
class ExportWorkflow:
    @workflow.run
    async def run(self, request: ExportRequest) -> ExportResult:
        return await workflow.execute_activity(
            export_rows,
            request,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(seconds=2),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100)),
        )
