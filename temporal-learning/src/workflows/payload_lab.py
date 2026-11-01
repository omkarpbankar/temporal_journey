"""Phase 12: the same job, passing data by value and then by reference."""

from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from shared.object_store import ObjectStore
    from shared.payloads import DatasetRef

TIMEOUT = dict(start_to_close_timeout=timedelta(seconds=30))


@dataclass
class PayloadActivities:
    store: ObjectStore

    @activity.defn
    def fetch_inline(self, rows: int) -> list[dict]:
        return [{"row": n, "value": "x" * 100} for n in range(rows)]

    @activity.defn
    def count_inline(self, rows: list[dict]) -> int:
        return len(rows)

    @activity.defn
    def fetch_to_store(self, rows: int) -> DatasetRef:
        data = [{"row": n, "value": "x" * 100} for n in range(rows)]
        return self.store.put_rows(f"{activity.info().workflow_id}/raw.json", data)

    @activity.defn
    def count_from_store(self, ref: DatasetRef) -> int:
        return len(self.store.get_rows(ref))


@workflow.defn
class PassByValueWorkflow:
    @workflow.run
    async def run(self, rows: int) -> int:
        data = await workflow.execute_activity_method(PayloadActivities.fetch_inline, rows, **TIMEOUT)
        return await workflow.execute_activity_method(PayloadActivities.count_inline, data, **TIMEOUT)


@workflow.defn
class PassByReferenceWorkflow:
    @workflow.run
    async def run(self, rows: int) -> int:
        ref = await workflow.execute_activity_method(PayloadActivities.fetch_to_store, rows, **TIMEOUT)
        return await workflow.execute_activity_method(PayloadActivities.count_from_store, ref, **TIMEOUT)
