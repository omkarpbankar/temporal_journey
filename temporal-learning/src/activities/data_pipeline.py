"""Phase 17: pipeline steps. Each one reads and writes objects, and passes references."""

from dataclasses import dataclass, field

from temporalio import activity
from temporalio.exceptions import ApplicationError

from shared.object_store import ObjectStore
from shared.payloads import DatasetRef


@dataclass
class PartitionRequest:
    run_id: str
    partition: str
    rows: int
    null_fraction: float = 0.0


@dataclass
class StageRequest:
    run_id: str
    refs: list[DatasetRef]


@dataclass
class DataActivities:
    store: ObjectStore
    # A lab switch: partitions in this set fail until a test removes them.
    broken_partitions: set[str] = field(default_factory=set)

    @activity.defn
    def extract(self, request: PartitionRequest) -> DatasetRef:
        nulls = int(request.rows * request.null_fraction)
        rows = [
            {"id": n, "partition": request.partition, "amount": None if n < nulls else n}
            for n in range(request.rows)
        ]
        return self.store.put_rows(f"{request.run_id}/raw/{request.partition}.json", rows)

    @activity.defn
    def validate(self, ref: DatasetRef) -> DatasetRef:
        rows = self.store.get_rows(ref)
        if not rows or "id" not in rows[0]:
            raise ApplicationError(f"{ref.uri}: bad schema", type="BadSchema", non_retryable=True)
        return ref

    @activity.defn
    def transform(self, request: PartitionRequest, ref: DatasetRef) -> DatasetRef:
        if request.partition in self.broken_partitions:
            raise RuntimeError(f"transform failed for {request.partition}")
        rows = [{**row, "doubled": None if row["amount"] is None else row["amount"] * 2} for row in self.store.get_rows(ref)]
        return self.store.put_rows(f"{request.run_id}/clean/{request.partition}.json", rows)

    @activity.defn
    def load_staging(self, request: StageRequest) -> DatasetRef:
        rows = [row for ref in request.refs for row in self.store.get_rows(ref)]
        return self.store.put_rows(f"{request.run_id}/staging.json", rows)

    @activity.defn
    def quality_check(self, ref: DatasetRef) -> float:
        rows = self.store.get_rows(ref)
        return sum(row["amount"] is not None for row in rows) / len(rows)

    @activity.defn
    def publish(self, ref: DatasetRef) -> str:
        run_id = ref.uri.split("/")[0]
        published = self.store.put_rows(f"published/{run_id}.json", self.store.get_rows(ref))
        return published.uri

    @activity.defn
    def delete_staging(self, run_id: str) -> None:
        self.store.delete(f"{run_id}/staging.json")
