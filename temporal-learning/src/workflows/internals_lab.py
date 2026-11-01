"""Phase 19: watch replay happen.

Sandboxing is off here so that module-level TRACE survives between Workflow Tasks. It
records each pass through the code. Never do this in real code: it is the exact shared
state the sandbox exists to prevent.
"""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow

TRACE: list[str] = []


@activity.defn
async def traced_step(name: str) -> str:
    return name


@workflow.defn(sandboxed=False)
class TracedWorkflow:
    @workflow.run
    async def run(self) -> str:
        def note(line: str) -> None:
            TRACE.append(f"{line} replaying={workflow.unsafe.is_replaying()}")

        note("line 1")
        await workflow.execute_activity(traced_step, "one", start_to_close_timeout=timedelta(seconds=5))
        note("line 2")
        await asyncio.sleep(1)
        note("line 3")
        await workflow.execute_activity(traced_step, "two", start_to_close_timeout=timedelta(seconds=5))
        note("line 4")
        return "done"
