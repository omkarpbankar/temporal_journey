"""Phase 4: Activities, the only place side effects belong.

These are sync methods, so the Worker runs them on a thread pool (its
activity_executor). A blocking call here is fine. The same blocking call in an
`async def` Activity would freeze the Worker's event loop.

Faults let labs break an Activity on demand. Production Activities don't have them.
"""

import os
import time
from dataclasses import dataclass, field

from temporalio import activity
from temporalio.exceptions import ApplicationError

from shared.ledger import Ledger, OutOfStock
from shared.models import ChargeRequest, Customer, EmailRequest, ReserveRequest


@dataclass
class Fault:
    """Misbehave on attempts 1..times.

    transient: fail before the side effect
    permanent: fail in a way retries cannot fix
    hang: sleep longer than the timeout
    lost_response: do the side effect, then fail, as if the reply was lost
    crash: do the side effect, then kill the whole Worker process
    """

    kind: str
    times: int = 1
    seconds: float = 0


@dataclass
class CommerceActivities:
    ledger: Ledger
    faults: dict[str, Fault] = field(default_factory=dict)

    def _before(self, name: str) -> Fault | None:
        fault = self.faults.get(name)
        if fault is None or activity.info().attempt > fault.times:
            return None
        if fault.kind == "transient":
            raise RuntimeError(f"{name}: temporary outage")
        if fault.kind == "permanent":
            raise ApplicationError(f"{name}: rejected", type="PermanentFailure", non_retryable=True)
        if fault.kind == "hang":
            time.sleep(fault.seconds)
        return fault

    @staticmethod
    def _after(name: str, fault: Fault | None) -> None:
        if fault is None:
            return
        if fault.kind == "lost_response":
            raise TimeoutError(f"{name}: the reply was lost after the change was committed")
        if fault.kind == "crash":
            os._exit(1)

    @activity.defn
    def create_customer(self, customer: Customer) -> str:
        fault = self._before("create_customer")
        result = self.ledger.apply("customer", customer.customer_id, customer.email, customer.customer_id)
        self._after("create_customer", fault)
        return result

    @activity.defn
    def charge_payment(self, request: ChargeRequest) -> str:
        fault = self._before("charge_payment")
        if request.amount_cents >= 1_000_000:
            # A business answer, not an outage. Retrying would only annoy the bank.
            raise ApplicationError("Card declined", type="PaymentDeclined", non_retryable=True)
        result = self.ledger.apply(
            "charge", request.order_id, str(request.amount_cents), request.idempotency_key
        )
        self._after("charge_payment", fault)
        return result

    @activity.defn
    def refund_payment(self, request: ChargeRequest) -> str:
        fault = self._before("refund_payment")
        result = self.ledger.apply(
            "refund", request.order_id, str(request.amount_cents), request.idempotency_key
        )
        self._after("refund_payment", fault)
        return result

    @activity.defn
    def reserve_inventory(self, request: ReserveRequest) -> str:
        fault = self._before("reserve_inventory")
        try:
            result = self.ledger.reserve(request.order_id, request.quantity, request.idempotency_key)
        except OutOfStock as error:
            raise ApplicationError(str(error), type="OutOfStock", non_retryable=True) from error
        self._after("reserve_inventory", fault)
        return result

    @activity.defn
    def release_inventory(self, request: ReserveRequest) -> None:
        fault = self._before("release_inventory")
        self.ledger.release(request.order_id, request.quantity, request.idempotency_key)
        self._after("release_inventory", fault)

    @activity.defn
    def send_email(self, request: EmailRequest) -> str:
        fault = self._before("send_email")
        result = self.ledger.apply("email", request.order_id, request.message, request.idempotency_key)
        self._after("send_email", fault)
        return result

    @activity.defn
    def write_database(self, order_id: str) -> str:
        fault = self._before("write_database")
        result = self.ledger.apply("db_row", order_id, "", f"{order_id}-row")
        self._after("write_database", fault)
        return result

    @activity.defn
    def call_external_api(self, order_id: str) -> str:
        fault = self._before("call_external_api")
        result = self.ledger.apply("api_call", order_id, "", f"{order_id}-api")
        self._after("call_external_api", fault)
        return result
