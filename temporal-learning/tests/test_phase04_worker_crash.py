"""The question from Phase 1, answered with a real process being really killed."""

import asyncio
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from temporalio.worker import Worker

from activities.commerce import CommerceActivities
from shared.models import CheckoutInput, Order
from workflows.checkout import CheckoutWorkflow

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.integration
@pytest.mark.parametrize("use_idempotency, charges", [(True, 1), (False, 2)])
async def test_worker_killed_after_the_charge_commits(local_env, queue, ledger, use_idempotency, charges):
    client = local_env.client
    process = subprocess.Popen(
        [sys.executable, "-m", "tests.helpers.crash_worker", client.service_client.config.target_host, queue, ledger.path],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": f"{ROOT / 'src'}{os.pathsep}{ROOT}"},
    )
    handle = await client.start_workflow(
        CheckoutWorkflow.run,
        CheckoutInput(Order("o-1", "c-1", "keyboard", 1, 5000), use_idempotency=use_idempotency, start_to_close_seconds=2),
        id=queue,
        task_queue=queue,
    )
    assert await asyncio.to_thread(process.wait, 30) == 1
    assert ledger.count("charge") == 1

    activities = CommerceActivities(ledger)
    async with Worker(
        client,
        task_queue=queue,
        workflows=[CheckoutWorkflow],
        activities=[activities.charge_payment, activities.reserve_inventory, activities.send_email],
        activity_executor=ThreadPoolExecutor(4),
    ):
        result = await asyncio.wait_for(handle.result(), 30)

    assert result.status == "completed"
    assert ledger.count("charge") == charges
