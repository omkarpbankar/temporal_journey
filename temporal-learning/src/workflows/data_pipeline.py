"""Phase 17: extract, validate, transform, load, check, publish.

What a scheduler-style DAG tool doesn't give you for free: this run can stop and wait
days for a human, redo only the partitions that failed, and undo its staging write.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, is_cancelled_exception

with workflow.unsafe.imports_passed_through():
    from activities.data_pipeline import DataActivities, PartitionRequest, StageRequest
    from shared.payloads import DatasetRef

TASK_QUEUE = "data-pipeline"
STEP = dict(
    start_to_close_timeout=timedelta(minutes=5),
    retry_policy=RetryPolicy(
        initial_interval=timedelta(milliseconds=50),
        maximum_interval=timedelta(milliseconds=200),
        maximum_attempts=2,
        non_retryable_error_types=["BadSchema"],
    ),
)


@dataclass
class PipelineInput:
    run_id: str
    partitions: list[str]
    rows_per_partition: int = 20
    null_fraction: float = 0.0
    quality_threshold: float = 0.9
    decision_hours: float = 48


@dataclass
class PipelineStatus:
    stage: str
    done: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    quality: float | None = None


@dataclass
class PipelineResult:
    status: str
    published_uri: str | None = None
    quality: float | None = None
    abandoned: list[str] = field(default_factory=list)


@workflow.defn
class DataPipelineWorkflow:
    def __init__(self) -> None:
        self.status_value = PipelineStatus("starting")
        self.retry_failed = False
        self.drop_failed = False
        self.approved: bool | None = None
        self.staged = False

    @workflow.signal
    def retry_failed_partitions(self) -> None:
        self.retry_failed = True

    @workflow.signal
    def drop_failed_partitions(self) -> None:
        self.drop_failed = True

    @workflow.signal
    def decide(self, approved: bool) -> None:
        self.approved = approved

    @workflow.query
    def status(self) -> PipelineStatus:
        return self.status_value

    async def _partition(self, input: PipelineInput, partition: str) -> DatasetRef:
        request = PartitionRequest(input.run_id, partition, input.rows_per_partition, input.null_fraction)
        raw = await workflow.execute_activity_method(DataActivities.extract, request, **STEP)
        valid = await workflow.execute_activity_method(DataActivities.validate, raw, **STEP)
        return await workflow.execute_activity_method(DataActivities.transform, args=[request, valid], **STEP)

    async def _process(self, input: PipelineInput, partitions: list[str]) -> dict[str, DatasetRef]:
        outcomes = await asyncio.gather(
            *(self._partition(input, partition) for partition in partitions), return_exceptions=True
        )
        done = {}
        self.status_value.failed = []
        for partition, outcome in zip(partitions, outcomes):
            if isinstance(outcome, BaseException) and is_cancelled_exception(outcome):
                raise outcome
            if isinstance(outcome, BaseException):
                self.status_value.failed.append(partition)
            else:
                done[partition] = outcome
                self.status_value.done.append(partition)
        return done

    async def _compensate(self, input: PipelineInput) -> None:
        if self.staged:
            await workflow.execute_activity_method(DataActivities.delete_staging, input.run_id, **STEP)

    @workflow.run
    async def run(self, input: PipelineInput) -> PipelineResult:
        decision_timeout = timedelta(hours=input.decision_hours)
        try:
            self.status_value.stage = "processing"
            refs = await self._process(input, input.partitions)

            # Partial failure: finished partitions keep their results. Only the rest rerun.
            while self.status_value.failed:
                self.status_value.stage = "awaiting_partition_decision"
                self.retry_failed = self.drop_failed = False
                try:
                    await workflow.wait_condition(
                        lambda: self.retry_failed or self.drop_failed, timeout=decision_timeout
                    )
                except asyncio.TimeoutError:
                    return PipelineResult("timed_out", abandoned=self.status_value.failed)
                if self.drop_failed:
                    break
                self.status_value.stage = "reprocessing"
                refs.update(await self._process(input, self.status_value.failed))

            abandoned = list(self.status_value.failed)
            if not refs:
                return PipelineResult("failed", abandoned=abandoned)

            self.status_value.stage = "loading"
            ordered = [refs[partition] for partition in input.partitions if partition in refs]
            staged = await workflow.execute_activity_method(
                DataActivities.load_staging, StageRequest(input.run_id, ordered), **STEP
            )
            self.staged = True

            self.status_value.stage = "quality_check"
            quality = await workflow.execute_activity_method(DataActivities.quality_check, staged, **STEP)
            self.status_value.quality = quality

            if quality < input.quality_threshold:
                self.status_value.stage = "awaiting_approval"
                try:
                    await workflow.wait_condition(lambda: self.approved is not None, timeout=decision_timeout)
                except asyncio.TimeoutError:
                    self.approved = False
                if not self.approved:
                    await self._compensate(input)
                    self.status_value.stage = "rejected"
                    return PipelineResult("rejected", quality=quality, abandoned=abandoned)

            self.status_value.stage = "publishing"
            uri = await workflow.execute_activity_method(DataActivities.publish, staged, **STEP)
            self.status_value.stage = "completed"
            return PipelineResult("completed", uri, quality, abandoned)
        except (asyncio.CancelledError, ActivityError) as error:
            if not is_cancelled_exception(error):
                raise
            self.status_value.stage = "cancelled"
            await asyncio.shield(self._compensate(input))
            raise
