"""Capstone Activities. Every one is safe to run twice, and that is tested under chaos.

File writes use a key derived from the request, so a repeat overwrites the same bytes.
Ledger writes carry an idempotency key, so a repeat returns the first result.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar

from temporalio import activity

from activities.commerce import CommerceActivities, Fault
from capstone.models import ChunkRequest, MergeRequest, MetadataUpdate, Notification, ProcessingRequest
from shared.ledger import Ledger
from shared.object_store import ObjectStore
from shared.payloads import DatasetRef

T = TypeVar("T")


@dataclass
class CapstoneActivities:
    store: ObjectStore
    ledger: Ledger
    faults: dict[str, Fault] = field(default_factory=dict)
    # How many readiness checks fail before the source appears. Stands in for a slow upstream.
    source_delay_checks: int = 0

    def _run(self, name: str, action: Callable[[], T]) -> T:
        chaos = CommerceActivities(self.ledger, self.faults)
        fault = chaos._before(name)
        result = action()
        chaos._after(name, fault)
        return result

    @activity.defn
    def create_record(self, request: ProcessingRequest) -> str:
        return self._run(
            "create_record",
            lambda: self.ledger.apply("record", request.request_id, request.source_name, f"{request.request_id}-record"),
        )

    @activity.defn
    def source_ready(self, source_name: str) -> bool:
        if self.source_delay_checks > 0:
            self.source_delay_checks -= 1
            return False
        return True

    @activity.defn
    def download_source(self, request: ProcessingRequest) -> DatasetRef:
        def download() -> DatasetRef:
            nulls = int(request.rows * request.null_fraction)
            rows = [{"id": n, "value": None if n < nulls else n} for n in range(request.rows)]
            activity.heartbeat("downloaded")
            return self.store.put_rows(f"{request.request_id}/source.json", rows)

        return self._run("download_source", download)

    @activity.defn
    def process_chunk(self, chunk: ChunkRequest) -> DatasetRef:
        def process() -> DatasetRef:
            rows = self.store.get_rows(chunk.source)[chunk.start : chunk.start + chunk.size]
            output = []
            for row in rows:
                output.append({**row, "score": None if row["value"] is None else row["value"] * 2})
                activity.heartbeat(row["id"])
            return self.store.put_rows(f"{chunk.request_id}/chunks/{chunk.index}.json", output)

        return self._run("process_chunk", process)

    @activity.defn
    def merge_chunks(self, request: MergeRequest) -> DatasetRef:
        rows = [row for ref in request.chunks for row in self.store.get_rows(ref)]
        return self.store.put_rows(f"{request.request_id}/staged.json", rows)

    @activity.defn
    def quality_check(self, ref: DatasetRef) -> float:
        rows = self.store.get_rows(ref)
        return sum(row["value"] is not None for row in rows) / len(rows)

    @activity.defn
    def store_results(self, ref: DatasetRef) -> DatasetRef:
        request_id = ref.uri.split("/")[0]
        return self._run(
            "store_results",
            lambda: self.store.put_rows(f"results/{request_id}.json", self.store.get_rows(ref)),
        )

    @activity.defn
    def update_metadata(self, update: MetadataUpdate) -> str:
        return self._run(
            "update_metadata",
            lambda: self.ledger.apply("metadata", update.request_id, update.output_uri, f"{update.request_id}-metadata"),
        )

    @activity.defn
    def publish_event(self, request_id: str) -> str:
        return self._run(
            "publish_event",
            lambda: self.ledger.apply("event", request_id, "completed", f"{request_id}-event"),
        )

    @activity.defn
    def notify_user(self, notification: Notification) -> str:
        return self._run(
            "notify_user",
            lambda: self.ledger.apply(
                "notification",
                notification.request_id,
                notification.email,
                f"{notification.request_id}-{notification.message}",
            ),
        )

    @activity.defn
    def write_audit(self, request_id: str) -> str:
        return self.ledger.apply("audit", request_id, "", f"{request_id}-audit")

    @activity.defn
    def delete_artifacts(self, request_id: str) -> None:
        for key in (f"{request_id}/staged.json", f"results/{request_id}.json"):
            self.store.delete(key)
        self.ledger.apply("cleanup", request_id, "", f"{request_id}-cleanup")
