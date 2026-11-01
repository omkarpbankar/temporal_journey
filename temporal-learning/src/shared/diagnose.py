"""Phase 13: answer "why is this Workflow stuck?" from Temporal's own records."""

from dataclasses import dataclass, field

from temporalio.api.enums.v1 import EventType, PendingActivityState
from temporalio.client import WorkflowExecutionStatus, WorkflowHandle

RETRY_STORM_ATTEMPTS = 5


@dataclass
class Diagnosis:
    status: str
    verdict: str
    evidence: list[str] = field(default_factory=list)


async def diagnose(handle: WorkflowHandle) -> Diagnosis:
    description = await handle.describe()
    status = description.status.name if description.status else "UNKNOWN"
    if description.status != WorkflowExecutionStatus.RUNNING:
        return Diagnosis(status, "CLOSED", [f"close_time={description.close_time}"])

    events = [event async for event in handle.fetch_history_events()]
    evidence = [f"history_length={len(events)}"]

    # Check this first. A failing Workflow Task blocks everything behind it.
    task_failures = [
        event.workflow_task_failed_event_attributes.failure.message
        for event in events
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
    ]
    if task_failures:
        evidence.append(f"workflow_task_failed: {task_failures[-1][:200]}")
        kind = "NONDETERMINISM" if "ondetermin" in task_failures[-1] else "WORKFLOW_TASK_FAILING"
        return Diagnosis(status, kind, evidence)

    pending = description.raw_description.pending_activities
    for activity in pending:
        name = activity.activity_type.name
        evidence.append(f"{name}: attempt={activity.attempt} state={PendingActivityState.Name(activity.state)}")
        if activity.attempt >= RETRY_STORM_ATTEMPTS:
            evidence.append(f"last_failure: {activity.last_failure.message}")
            return Diagnosis(status, "RETRY_STORM", evidence)
        if activity.state == PendingActivityState.PENDING_ACTIVITY_STATE_SCHEDULED and activity.attempt == 1:
            return Diagnosis(status, "NO_ACTIVITY_WORKER", evidence)
    if pending:
        return Diagnosis(status, "ACTIVITY_RUNNING", evidence)

    last = events[-1].event_type
    if last == EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
        return Diagnosis(status, "NO_WORKFLOW_WORKER", evidence)
    return Diagnosis(status, "WAITING_FOR_TIMER_OR_MESSAGE", evidence)
