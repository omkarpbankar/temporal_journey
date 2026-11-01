"""Phase 9: one program, started with a different role per pool.

    python -m workers.specialized --role payments --concurrency 10
    python -m workers.specialized --role notifications --concurrency 100

Run as many copies of each role as that workload needs. They share nothing but
the Task Queue name.
"""

import argparse
import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import SharedStateManager, Worker

from activities.commerce import CommerceActivities
from activities.cpu_heavy import hash_block
from shared.connection import connect
from shared.ledger import Ledger
from workflows.routed_order import (
    DATA_PROCESSING,
    INVENTORY,
    NOTIFICATIONS,
    ORDERS,
    PAYMENTS,
    RoutedOrderWorkflow,
)


def build_worker(client: Client, role: str, ledger: Ledger, concurrency: int, prefix: str = "") -> Worker:
    commerce = CommerceActivities(ledger)
    threads = ThreadPoolExecutor(max(concurrency, 1))
    if role == "orders":
        # Workflow Workers do no I/O, so they can be many and cheap.
        return Worker(client, task_queue=prefix + ORDERS, workflows=[RoutedOrderWorkflow])
    if role == "payments":
        activities = [commerce.charge_payment, commerce.refund_payment]
    elif role == "inventory":
        activities = [commerce.reserve_inventory, commerce.release_inventory]
    elif role == "notifications":
        activities = [commerce.send_email]
    elif role == "data-processing":
        # One process per core, because threads cannot run Python code in parallel.
        return Worker(
            client,
            task_queue=prefix + DATA_PROCESSING,
            activities=[hash_block],
            activity_executor=ProcessPoolExecutor(concurrency),
            shared_state_manager=SharedStateManager.create_from_multiprocessing(multiprocessing.Manager()),
            max_concurrent_activities=concurrency,
        )
    else:
        raise ValueError(f"unknown role: {role}")

    queue = {"payments": PAYMENTS, "inventory": INVENTORY, "notifications": NOTIFICATIONS}[role]
    return Worker(
        client,
        task_queue=prefix + queue,
        activities=activities,
        activity_executor=threads,
        max_concurrent_activities=concurrency,
    )


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--ledger", default="routing-ledger.db")
    args = parser.parse_args()

    client = await connect()
    print(f"Starting {args.role} worker with concurrency {args.concurrency}")
    await build_worker(client, args.role, Ledger(args.ledger), args.concurrency).run()


if __name__ == "__main__":
    asyncio.run(main())
