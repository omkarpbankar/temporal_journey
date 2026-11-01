"""Run with: python -m workers.hello_worker"""

import asyncio
import logging

from temporalio.worker import Worker

from activities.greetings import compose_greeting
from shared.connection import connect
from workflows.hello import TASK_QUEUE, HelloWorkflow, SlowHelloWorkflow


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    client = await connect()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[HelloWorkflow, SlowHelloWorkflow],
        activities=[compose_greeting],
    )
    print(f"Worker polling task queue '{TASK_QUEUE}'. Press Ctrl+C to stop.")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
