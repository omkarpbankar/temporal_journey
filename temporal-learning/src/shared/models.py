"""Data that crosses the Temporal boundary. Everything here must serialize to JSON."""

from dataclasses import dataclass, field


@dataclass
class Order:
    order_id: str
    customer_id: str
    item: str
    quantity: int
    amount_cents: int
    address: str = "1 Main St"


@dataclass
class Customer:
    customer_id: str
    email: str


def validate_order(order: Order) -> list[str]:
    """Pure logic: same input, same output, no I/O. Safe to run inside a Workflow."""
    errors = []
    if order.quantity <= 0:
        errors.append("quantity must be positive")
    if order.amount_cents <= 0:
        errors.append("amount must be positive")
    if not order.item:
        errors.append("item is required")
    return errors


@dataclass
class ChargeRequest:
    order_id: str
    amount_cents: int
    idempotency_key: str | None = None


@dataclass
class ReserveRequest:
    order_id: str
    item: str
    quantity: int
    idempotency_key: str | None = None


@dataclass
class EmailRequest:
    order_id: str
    message: str
    idempotency_key: str | None = None


@dataclass
class CheckoutInput:
    order: Order
    use_idempotency: bool = True
    max_attempts: int = 5
    start_to_close_seconds: float = 5


@dataclass
class CheckoutResult:
    order_id: str
    status: str
    charge_id: str | None = None
    reason: str | None = None
    steps: list[str] = field(default_factory=list)
