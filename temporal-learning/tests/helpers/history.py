"""Read a Workflow's Event History, the same data the Web UI shows."""

from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowHandle


async def event_names(handle: WorkflowHandle) -> list[str]:
    return [
        EventType.Name(event.event_type).removeprefix("EVENT_TYPE_")
        async for event in handle.fetch_history_events()
    ]


async def workflow_task_failure(handle: WorkflowHandle) -> str | None:
    async for event in handle.fetch_history_events():
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED:
            return event.workflow_task_failed_event_attributes.failure.message
    return None
