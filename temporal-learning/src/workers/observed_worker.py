"""Phase 13: a Worker that exposes Prometheus metrics.

    python -m workers.observed_worker
    Then open http://localhost:9000/metrics
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

from activities.commerce import CommerceActivities
from shared.connection import connect
from shared.ledger import Ledger
from workflows.checkout import TASK_QUEUE, CheckoutWorkflow


def metrics_runtime(bind_address: str = "0.0.0.0:9000") -> Runtime:
    return Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address=bind_address)))


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    client = await connect(runtime=metrics_runtime())
    activities = CommerceActivities(Ledger("observed-ledger.db"))
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[CheckoutWorkflow],
        activities=[activities.charge_payment, activities.reserve_inventory, activities.send_email],
        activity_executor=ThreadPoolExecutor(10),
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
