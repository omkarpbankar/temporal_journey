"""Phase 18: reusable patterns."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow

STEP = dict(start_to_close_timeout=timedelta(seconds=30))


class Saga:
    """Remember how to undo each finished step, then undo them newest-first.

    The list lives in Workflow memory, so it is rebuilt by replay after a crash.
    That is the part you would otherwise need a database table for.
    """

    def __init__(self) -> None:
        self._undo: list[Callable[[], Awaitable[None]]] = []

    def add(self, undo: Callable[[], Awaitable[None]]) -> None:
        self._undo.append(undo)

    async def compensate(self) -> list[str]:
        failures = []
        for undo in reversed(self._undo):
            # One failed undo must not stop the others. Surface it for a human instead.
            try:
                await undo()
            except Exception as error:
                failures.append(str(error))
        return failures


@activity.defn
async def process_batch(items: list[str]) -> int:
    return len(items)


@activity.defn
async def tick(label: str) -> str:
    return label


@dataclass
class BatchInput:
    items: list[str]
    batch_size: int = 10
    batches_per_run: int = 5
    processed: int = 0


@workflow.defn
class BatchWorkflow:
    """Work through a long list in chunks, without growing one huge history."""

    @workflow.run
    async def run(self, input: BatchInput) -> int:
        processed = input.processed
        for _ in range(input.batches_per_run):
            batch = input.items[processed : processed + input.batch_size]
            if not batch:
                return processed
            processed += await workflow.execute_activity(process_batch, batch, **STEP)
        if processed < len(input.items):
            workflow.continue_as_new(
                BatchInput(input.items, input.batch_size, input.batches_per_run, processed)
            )
        return processed


@workflow.defn
class ReportWorkflow:
    """What a Schedule starts."""

    @workflow.run
    async def run(self, label: str) -> str:
        return await workflow.execute_activity(tick, label, **STEP)


@workflow.defn
class WaiterWorkflow:
    def __init__(self) -> None:
        self.payload: str | None = None

    @workflow.signal
    def deliver(self, payload: str) -> None:
        self.payload = payload

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self.payload is not None)
        return self.payload


@workflow.defn
class NotifierWorkflow:
    """One Workflow coordinating another, by ID, with no shared code path."""

    @workflow.run
    async def run(self, target_id: str) -> None:
        await workflow.get_external_workflow_handle(target_id).signal(WaiterWorkflow.deliver, "ready")


@dataclass
class ApprovalChain:
    approvers: list[str]
    hours_each: float = 24


@dataclass
class ApprovalState:
    approved_by: list[str] = field(default_factory=list)
    escalated: bool = False


@workflow.defn
class HumanApprovalWorkflow:
    """Human-in-the-loop: wait, remind by escalating, and never lose your place."""

    def __init__(self) -> None:
        self.state = ApprovalState()
        self.pending: set[str] = set()

    @workflow.signal
    def approve(self, approver: str) -> None:
        self.pending.add(approver)

    @workflow.run
    async def run(self, chain: ApprovalChain) -> ApprovalState:
        for approver in chain.approvers:
            try:
                await workflow.wait_condition(
                    lambda: approver in self.pending, timeout=timedelta(hours=chain.hours_each)
                )
                self.state.approved_by.append(approver)
            except asyncio.TimeoutError:
                self.state.escalated = True
                return self.state
        return self.state
