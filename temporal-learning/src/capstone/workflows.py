"""Capstone Workflows: one request from validation to notification."""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    ChildWorkflowError,
    is_cancelled_exception,
)

with workflow.unsafe.imports_passed_through():
    from capstone.activities import CapstoneActivities
    from capstone.models import (
        DATA,
        IO,
        ChunkJob,
        ChunkRequest,
        MergeRequest,
        MetadataUpdate,
        Notification,
        ProcessingRequest,
        ProcessingResult,
        ProcessingStatus,
    )
    from shared.payloads import DatasetRef

RETRY = RetryPolicy(
    initial_interval=timedelta(milliseconds=100),
    maximum_interval=timedelta(seconds=2),
    maximum_attempts=6,
)
SOURCE_CHECK_SECONDS = 60
SOURCE_CHECKS = 30


def options(request: ProcessingRequest, queue: str) -> dict:
    return dict(
        task_queue=request.queue_prefix + queue,
        start_to_close_timeout=timedelta(seconds=request.activity_timeout_seconds),
        retry_policy=RETRY,
    )


@workflow.defn
class ChunkProcessingWorkflow:
    """Unbounded work in bounded history: roll to a fresh run every few chunks."""

    @workflow.run
    async def run(self, job: ChunkJob) -> list[DatasetRef]:
        request, finished = job.request, list(job.finished)
        index = job.next_index
        total = (request.rows + request.chunk_size - 1) // request.chunk_size

        for _ in range(request.chunks_per_run):
            if index >= total:
                return finished
            chunk = ChunkRequest(request.request_id, index, job.source, index * request.chunk_size, request.chunk_size)
            finished.append(
                await workflow.execute_activity_method(
                    CapstoneActivities.process_chunk,
                    chunk,
                    heartbeat_timeout=timedelta(seconds=request.activity_timeout_seconds),
                    **options(request, DATA),
                )
            )
            index += 1

        if index >= total:
            return finished
        workflow.continue_as_new(ChunkJob(request, job.source, index, finished))


@workflow.defn
class DataRequestWorkflow:
    # Subclassed in tests to stand in for the code as it was before write_audit was added.
    audit_enabled = True

    @workflow.init
    def __init__(self, request: ProcessingRequest) -> None:
        self.state = "RECEIVED"
        self.notify_email = request.notify_email
        self.quality: float | None = None
        self.chunks_done = 0
        self.error: str | None = None
        self.approved: bool | None = None

    @workflow.signal
    def decide(self, approved: bool) -> None:
        self.approved = approved

    @workflow.query
    def status(self) -> ProcessingStatus:
        return ProcessingStatus(self.state, self.notify_email, self.chunks_done, self.quality, self.error)

    @workflow.update
    def change_notify_email(self, email: str) -> str:
        previous, self.notify_email = self.notify_email, email
        return previous

    @change_notify_email.validator
    def check_email(self, email: str) -> None:
        if "@" not in email:
            raise ValueError("that is not an email address")
        if self.state in ("COMPLETED", "FAILED", "CANCELLED", "REJECTED"):
            raise ValueError(f"request is already {self.state}")

    async def _wait_for_source(self, request: ProcessingRequest) -> None:
        self.state = "WAITING_FOR_SOURCE"
        for _ in range(SOURCE_CHECKS):
            if await workflow.execute_activity_method(
                CapstoneActivities.source_ready, request.source_name, **options(request, IO)
            ):
                return
            # A durable timer, so a source that is days late costs nothing while waiting.
            await asyncio.sleep(SOURCE_CHECK_SECONDS)
        raise ApplicationError(f"{request.source_name} never appeared", type="SourceMissing")

    async def _notify(self, request: ProcessingRequest, message: str) -> None:
        await workflow.execute_activity_method(
            CapstoneActivities.notify_user,
            Notification(request.request_id, self.notify_email, message),
            **options(request, IO),
        )

    async def _audit(self, request: ProcessingRequest) -> None:
        # Added after launch. patched() keeps runs started before this line replayable.
        if self.audit_enabled and workflow.patched("add-audit-log"):
            await workflow.execute_activity_method(
                CapstoneActivities.write_audit, request.request_id, **options(request, IO)
            )

    async def _cleanup(self, request: ProcessingRequest) -> None:
        await workflow.execute_activity_method(
            CapstoneActivities.delete_artifacts, request.request_id, **options(request, IO)
        )

    async def _process(self, request: ProcessingRequest) -> ProcessingResult:
        self.state = "CREATING_RECORD"
        await workflow.execute_activity_method(CapstoneActivities.create_record, request, **options(request, IO))

        await self._wait_for_source(request)

        self.state = "DOWNLOADING"
        source = await workflow.execute_activity_method(
            CapstoneActivities.download_source,
            request,
            heartbeat_timeout=timedelta(seconds=request.activity_timeout_seconds),
            **options(request, IO),
        )

        self.state = "PROCESSING"
        chunks = await workflow.execute_child_workflow(
            ChunkProcessingWorkflow.run,
            ChunkJob(request, source),
            id=f"{workflow.info().workflow_id}-chunks",
        )
        self.chunks_done = len(chunks)

        staged = await workflow.execute_activity_method(
            CapstoneActivities.merge_chunks, MergeRequest(request.request_id, chunks), **options(request, DATA)
        )

        self.state = "QUALITY_CHECK"
        self.quality = await workflow.execute_activity_method(
            CapstoneActivities.quality_check, staged, **options(request, DATA)
        )
        if self.quality < request.quality_threshold:
            self.state = "AWAITING_APPROVAL"
            try:
                await workflow.wait_condition(
                    lambda: self.approved is not None, timeout=timedelta(hours=request.approval_hours)
                )
            except asyncio.TimeoutError:
                self.approved = False
            if not self.approved:
                await self._cleanup(request)
                self.state = "REJECTED"
                await self._notify(request, "rejected")
                return ProcessingResult("REJECTED", quality=self.quality)

        self.state = "STORING"
        output = await workflow.execute_activity_method(
            CapstoneActivities.store_results, staged, **options(request, IO)
        )

        self.state = "PUBLISHING"
        await workflow.execute_activity_method(
            CapstoneActivities.update_metadata,
            MetadataUpdate(request.request_id, output.uri, output.rows),
            **options(request, IO),
        )
        await self._audit(request)
        await workflow.execute_activity_method(
            CapstoneActivities.publish_event, request.request_id, **options(request, IO)
        )
        await self._notify(request, "completed")

        # Let any Update already in progress finish before the run closes.
        await workflow.wait_condition(workflow.all_handlers_finished)
        self.state = "COMPLETED"
        workflow.metric_meter().create_counter("data_requests_completed").add(1)
        return ProcessingResult("COMPLETED", output.uri, self.quality, output.rows)

    @workflow.run
    async def run(self, request: ProcessingRequest) -> ProcessingResult:
        if request.rows <= 0 or request.chunk_size <= 0:
            self.state = "REJECTED"
            raise ApplicationError("rows and chunk_size must be positive", type="InvalidRequest")

        try:
            return await self._process(request)
        except (asyncio.CancelledError, ActivityError, ChildWorkflowError, ApplicationError) as error:
            cancelled = is_cancelled_exception(error)
            self.error = None if cancelled else str(error.cause or error)
            self.state = "CANCELLING" if cancelled else "CLEANING_UP"
            await asyncio.shield(self._cleanup(request))
            self.state = "CANCELLED" if cancelled else "FAILED"
            if not cancelled:
                await self._notify(request, "failed")
            raise
