"""Capstone: what crosses the Temporal boundary. References and small facts only."""

from dataclasses import dataclass, field

from shared.payloads import DatasetRef

ORCHESTRATION = "capstone-orchestration"
DATA = "capstone-data"
IO = "capstone-io"


@dataclass
class ProcessingRequest:
    request_id: str
    source_name: str
    notify_email: str
    rows: int = 100
    chunk_size: int = 25
    null_fraction: float = 0.0
    quality_threshold: float = 0.9
    approval_hours: float = 24
    chunks_per_run: int = 2
    activity_timeout_seconds: float = 30
    queue_prefix: str = ""


@dataclass
class ChunkRequest:
    request_id: str
    index: int
    source: DatasetRef
    start: int
    size: int


@dataclass
class ChunkJob:
    request: ProcessingRequest
    source: DatasetRef
    # Carried across Continue-As-New.
    next_index: int = 0
    finished: list[DatasetRef] = field(default_factory=list)


@dataclass
class MergeRequest:
    request_id: str
    chunks: list[DatasetRef]


@dataclass
class MetadataUpdate:
    request_id: str
    output_uri: str
    rows: int


@dataclass
class Notification:
    request_id: str
    email: str
    message: str


@dataclass
class ProcessingStatus:
    state: str
    notify_email: str
    chunks_done: int = 0
    quality: float | None = None
    error: str | None = None


@dataclass
class ProcessingResult:
    state: str
    output_uri: str | None = None
    quality: float | None = None
    rows: int = 0
