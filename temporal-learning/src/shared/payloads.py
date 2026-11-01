"""Phase 12: payload design. Temporal carries small facts, not big data."""

from dataclasses import dataclass, field
from enum import Enum


class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class CustomerRef:
    """An ID, not a profile. Names and emails stay in your database, away from history."""

    customer_id: str


@dataclass
class OrderLine:
    sku: str
    quantity: int
    unit_price_cents: int


@dataclass
class OrderPayload:
    order_id: str
    customer: CustomerRef
    lines: list[OrderLine]
    # New fields need defaults, so that history written before they existed still decodes.
    currency: str = "USD"


@dataclass
class PaymentPayload:
    payment_id: str
    order_id: str
    amount_cents: int
    # Never a card number. The provider's token is all a Workflow should ever see.
    provider_token: str


@dataclass
class DatasetRef:
    """A claim check: enough to find the data and to prove it hasn't changed."""

    uri: str
    rows: int
    size_bytes: int
    sha256: str


@dataclass
class DataPipelineRun:
    run_id: str
    source: DatasetRef
    partition: str
    output: DatasetRef | None = None


@dataclass
class JobStatus:
    job_id: str
    state: JobState
    progress_percent: int = 0
    errors: list[str] = field(default_factory=list)
