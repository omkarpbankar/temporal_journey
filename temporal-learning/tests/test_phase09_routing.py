import asyncio
import time

import pytest
from temporalio.client import Client
from temporalio.runtime import Runtime, TelemetryConfig
from temporalio.worker import Worker

from activities.cpu_heavy import slow_call
from shared.models import Order
from workers.specialized import build_worker
from workflows.routed_order import (
    ORDERS,
    BurstWorkflow,
    CpuJob,
    CpuJobWorkflow,
    RoutedOrderInput,
    RoutedOrderWorkflow,
)

pytestmark = pytest.mark.integration
ORDER = Order("o-1", "c-1", "keyboard", 1, 5000)


async def wait_for_stage(handle, stage):
    for _ in range(200):
        if await handle.query(RoutedOrderWorkflow.current_stage) == stage:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"never reached {stage}")


async def test_every_step_runs_on_its_own_queue(local_env, queue, ledger):
    client, prefix = local_env.client, f"{queue}-"
    workers = [build_worker(client, r, ledger, 2, prefix) for r in ("orders", "payments", "inventory", "notifications")]
    async with workers[0], workers[1], workers[2], workers[3]:
        result = await client.execute_workflow(
            RoutedOrderWorkflow.run, RoutedOrderInput(ORDER, prefix), id=queue, task_queue=prefix + ORDERS
        )
    assert result == "completed"


async def test_notification_outage_cannot_touch_payments(local_env, queue, ledger):
    client, prefix = local_env.client, f"{queue}-"
    orders, payments, inventory = (build_worker(client, r, ledger, 2, prefix) for r in ("orders", "payments", "inventory"))
    async with orders, payments, inventory:
        handle = await client.start_workflow(
            RoutedOrderWorkflow.run, RoutedOrderInput(ORDER, prefix), id=queue, task_queue=prefix + ORDERS
        )
        await wait_for_stage(handle, "notifying")
        # Money taken and stock held, with not one Notification Worker alive.
        assert ledger.count("charge") == 1 and ledger.count("reservation") == 1

        async with build_worker(client, "notifications", ledger, 2, prefix):
            assert await handle.result() == "completed"
    assert ledger.count("email") == 1


async def test_more_workers_drain_the_queue_faster(local_env, queue):
    target = local_env.client.service_client.config.target_host

    async def timed(worker_count: int, suffix: str) -> float:
        name = f"{queue}-{suffix}"
        workers = []
        for _ in range(worker_count):
            # The SDK allows one Worker per queue per Runtime. A separate Runtime stands
            # in for what you'd really run: a separate process.
            client = await Client.connect(target, runtime=Runtime(telemetry=TelemetryConfig()))
            workers.append(
                Worker(client, task_queue=name, workflows=[BurstWorkflow], activities=[slow_call], max_concurrent_activities=1)
            )
        for worker in workers:
            await worker.__aenter__()
        started = time.monotonic()
        await local_env.client.execute_workflow(BurstWorkflow.run, 6, id=name, task_queue=name)
        elapsed = time.monotonic() - started
        for worker in workers:
            await worker.__aexit__(None, None, None)
        return elapsed

    one = await timed(1, "one")
    three = await timed(3, "three")
    print(f"OBSERVED 6 tasks: 1 worker {one:.2f}s, 3 workers {three:.2f}s")
    assert three < one


async def test_cpu_work_runs_in_a_process_pool(local_env, queue, ledger):
    client, prefix = local_env.client, f"{queue}-"
    async with build_worker(client, "data-processing", ledger, 2, prefix):
        async with Worker(client, task_queue=queue, workflows=[CpuJobWorkflow]):
            result = await client.execute_workflow(CpuJobWorkflow.run, CpuJob(1000, prefix), id=queue, task_queue=queue)
    assert len(result) == 64
