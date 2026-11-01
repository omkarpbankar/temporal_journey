"""Save a real Workflow history for replay tests.

    python scripts/record_history.py

Runs HelloWorkflow and SlowHelloWorkflow once and writes their histories to
tests/histories. In production you'd download real histories instead:
handle.fetch_history() or `temporal workflow show --output json`.
"""

import asyncio
import json
import uuid
from pathlib import Path

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities.greetings import GreetingInput, compose_greeting
from workflows.hello import HelloWorkflow, SlowHelloWorkflow

OUTPUT = Path(__file__).resolve().parents[1] / "tests" / "histories"


async def record(env: WorkflowEnvironment, workflow_class: type) -> None:
    queue = f"record-{uuid.uuid4().hex[:8]}"
    async with Worker(env.client, task_queue=queue, workflows=[workflow_class], activities=[compose_greeting]):
        handle = await env.client.start_workflow(
            workflow_class.run, GreetingInput("Asha"), id=queue, task_queue=queue
        )
        await handle.result()
    history = await handle.fetch_history()
    record_file = {
        "workflow_id": queue,
        "workflow_type": workflow_class.__name__,
        "history": history.to_json(),
    }
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / f"{workflow_class.__name__}.json").write_text(json.dumps(record_file))


async def main() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        await record(env, HelloWorkflow)
        await record(env, SlowHelloWorkflow)
    print(f"Saved to {OUTPUT}")


if __name__ == "__main__":
    asyncio.run(main())
