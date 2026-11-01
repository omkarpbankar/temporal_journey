"""Run with: python -m clients.hello_client Omkar
Add --slow to run the version with a 30 second timer in the middle."""

import argparse
import asyncio
import uuid

from activities.greetings import GreetingInput
from shared.connection import connect
from workflows.hello import TASK_QUEUE, HelloWorkflow, SlowHelloWorkflow


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    parser.add_argument("--language", default="en")
    parser.add_argument("--slow", action="store_true")
    args = parser.parse_args()

    client = await connect()
    workflow_run = SlowHelloWorkflow.run if args.slow else HelloWorkflow.run
    handle = await client.start_workflow(
        workflow_run,
        GreetingInput(args.name, args.language),
        id=f"hello-{uuid.uuid4().hex[:8]}",
        task_queue=TASK_QUEUE,
    )
    print(f"Started {handle.id}. Watch it at http://localhost:8233")
    print("Result:", await handle.result())


if __name__ == "__main__":
    asyncio.run(main())
