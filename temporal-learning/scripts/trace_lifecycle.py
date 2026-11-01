"""Phase 19: run one Workflow and explain every event in its history.

    python scripts/trace_lifecycle.py
"""

import asyncio
import uuid

from temporalio.api.enums.v1 import EventType
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from workflows.internals_lab import TRACE, TracedWorkflow, traced_step

EXPLANATIONS = {
    "WORKFLOW_EXECUTION_STARTED": "Client asked for a run. The Service wrote it down first, then acted.",
    "WORKFLOW_TASK_SCHEDULED": "The Service put 'run some Workflow code' on the Task Queue.",
    "WORKFLOW_TASK_STARTED": "A Worker polling that queue picked the task up.",
    "WORKFLOW_TASK_COMPLETED": "The Worker reported the Commands the code produced.",
    "ACTIVITY_TASK_SCHEDULED": "A ScheduleActivityTask Command became an event, and a task joined the queue.",
    "ACTIVITY_TASK_STARTED": "A Worker took the Activity. Written when the Activity closes, with the final attempt.",
    "ACTIVITY_TASK_COMPLETED": "The result is now durable. The Activity will not run again.",
    "TIMER_STARTED": "A StartTimer Command. The Service owns this clock, not your process.",
    "TIMER_FIRED": "The time passed. The Service schedules a Workflow Task to wake the code.",
    "WORKFLOW_EXECUTION_COMPLETED": "The final Command. The return value is recorded and the run closes.",
}


async def main() -> None:
    queue = f"trace-{uuid.uuid4().hex[:8]}"
    async with await WorkflowEnvironment.start_time_skipping() as env:
        # With no cache, every Workflow Task replays from the top, so you can see it happen.
        async with Worker(
            env.client,
            task_queue=queue,
            workflows=[TracedWorkflow],
            activities=[traced_step],
            max_cached_workflows=0,
        ):
            handle = await env.client.start_workflow(TracedWorkflow.run, id=queue, task_queue=queue)
            await handle.result()

        print("EVENT HISTORY")
        async for event in handle.fetch_history_events():
            name = EventType.Name(event.event_type).removeprefix("EVENT_TYPE_")
            print(f"{event.event_id:>3} {name:<32} {EXPLANATIONS.get(name, '')}")

    print("\nWHAT THE PYTHON CODE DID")
    for line in TRACE:
        print(f"  {line}")


if __name__ == "__main__":
    asyncio.run(main())
