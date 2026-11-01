"""Phase 5: one Workflow per kind of failure, so you can see each one's exact shape."""

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

TASK_QUEUE = "failure-lab"
FAST_RETRY = dict(initial_interval=timedelta(milliseconds=50), maximum_interval=timedelta(milliseconds=100))


@dataclass
class FailureReport:
    outer: str
    cause: str
    retry_state: str | None


@activity.defn
async def always_fails() -> None:
    raise RuntimeError("the dependency is down")


@activity.defn
async def rejected() -> None:
    raise ApplicationError("invalid card", type="BadRequest", non_retryable=True)


@activity.defn
async def too_slow(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _report(error: ActivityError) -> FailureReport:
    return FailureReport(
        outer=type(error).__name__,
        cause=type(error.cause).__name__,
        retry_state=error.retry_state.name if error.retry_state else None,
    )


@workflow.defn
class RetryExhaustedWorkflow:
    @workflow.run
    async def run(self, max_attempts: int) -> FailureReport:
        try:
            await workflow.execute_activity(
                always_fails,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(maximum_attempts=max_attempts, **FAST_RETRY),
            )
        except ActivityError as error:
            return _report(error)
        raise ApplicationError("always_fails unexpectedly succeeded")


@workflow.defn
class NonRetryableWorkflow:
    @workflow.run
    async def run(self) -> FailureReport:
        try:
            await workflow.execute_activity(
                rejected,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(maximum_attempts=10, **FAST_RETRY),
            )
        except ActivityError as error:
            return _report(error)
        raise ApplicationError("rejected unexpectedly succeeded")


@workflow.defn
class ActivityTimeoutWorkflow:
    @workflow.run
    async def run(self) -> FailureReport:
        try:
            await workflow.execute_activity(
                too_slow,
                10.0,
                start_to_close_timeout=timedelta(milliseconds=300),
                retry_policy=RetryPolicy(maximum_attempts=2, **FAST_RETRY),
            )
        except ActivityError as error:
            return _report(error)
        raise ApplicationError("too_slow unexpectedly succeeded")


@workflow.defn
class BusinessFailureWorkflow:
    """Raising ApplicationError on purpose fails the Workflow Execution, permanently."""

    @workflow.run
    async def run(self) -> None:
        raise ApplicationError("Order violates policy", type="PolicyViolation")


@workflow.defn
class PythonBugWorkflow:
    """A plain Python exception does NOT fail the Workflow. It fails the Workflow Task.

    Temporal assumes it's a bug you can fix, so it keeps the Workflow open and
    retries the task. Deploying a fix can rescue the run.
    """

    @workflow.run
    async def run(self) -> str:
        raise KeyError("this is a typo-level bug")


@workflow.defn
class WaitsForeverWorkflow:
    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: False)
        return "never"
