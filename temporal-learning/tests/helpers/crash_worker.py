"""A real Worker process that dies right after the bank commits the charge.

python -m tests.helpers.crash_worker <address> <queue> <ledger path>
"""

import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from activities.commerce import CommerceActivities, Fault
from shared.ledger import Ledger
from workflows.checkout import CheckoutWorkflow


async def main(address: str, queue: str, ledger_path: str) -> None:
    client = await Client.connect(address)
    activities = CommerceActivities(Ledger(ledger_path), {"charge_payment": Fault("crash", 1)})
    worker = Worker(
        client,
        task_queue=queue,
        workflows=[CheckoutWorkflow],
        activities=[activities.charge_payment, activities.reserve_inventory, activities.send_email],
        activity_executor=ThreadPoolExecutor(4),
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:4]))
