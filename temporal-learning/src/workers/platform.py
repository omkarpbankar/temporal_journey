"""Phase 18: four Worker types for the fulfillment platform.

    python -m workers.platform --role orders
    python -m workers.platform --role payments
    python -m workers.platform --role fulfillment
    python -m workers.platform --role notifications
"""

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio.client import Client
from temporalio.worker import Worker

from activities.commerce import CommerceActivities
from shared.connection import connect
from shared.ledger import Ledger
from workflows.fulfillment_platform import (
    FULFILLMENT,
    NOTIFICATIONS,
    ORDERS,
    PAYMENTS,
    OrderFulfillmentWorkflow,
    PaymentChildWorkflow,
    PlatformActivities,
)


def build_worker(client: Client, role: str, platform: PlatformActivities, prefix: str = "") -> Worker:
    commerce = platform.commerce
    settings = {
        "client": client,
        # Lets running Activities finish during a rolling deploy instead of being cut off.
        "graceful_shutdown_timeout": timedelta(seconds=5),
    }
    if role == "orders":
        return Worker(task_queue=prefix + ORDERS, workflows=[OrderFulfillmentWorkflow, PaymentChildWorkflow], **settings)

    activities = {
        "payments": (PAYMENTS, [platform.fraud_check, commerce.charge_payment, commerce.refund_payment]),
        "fulfillment": (FULFILLMENT, [commerce.reserve_inventory, commerce.release_inventory, platform.ship]),
        "notifications": (NOTIFICATIONS, [commerce.send_email]),
    }
    queue, role_activities = activities[role]
    return Worker(
        task_queue=prefix + queue,
        activities=role_activities,
        activity_executor=ThreadPoolExecutor(20),
        max_concurrent_activities=20,
        **settings,
    )


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, choices=["orders", "payments", "fulfillment", "notifications"])
    parser.add_argument("--ledger", default="platform-ledger.db")
    args = parser.parse_args()
    client = await connect()
    platform = PlatformActivities(CommerceActivities(Ledger(args.ledger)))
    await build_worker(client, args.role, platform).run()


if __name__ == "__main__":
    asyncio.run(main())
