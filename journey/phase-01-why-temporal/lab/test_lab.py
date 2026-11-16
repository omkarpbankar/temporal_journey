"""Comprehensive verification test suite for Phase 1 Lab implementations.

Tests:
1. checkout_v1 (happy path, double charge vulnerability)
2. checkout_v2 (checkpoint recovery, idempotency key protection)
3. usual_fixes (retry transient blips, state machine DB gap, queue visibility timeout duplicate)
4. durable_execution (event history logging, replay execution skipping completed activities)
5. the_cast (decoupled Client/Service/TaskQueue/Worker, zero-worker queue buffering)

Run with:
    pytest test_lab.py -v
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import world
from checkout_v2 import checkout as checkout_v2
from durable_execution import EventHistory, checkout_workflow
from the_cast import Client, TemporalService, Worker
from usual_fixes import recover_state_machine, run_queue_pipeline, run_retries_demo, run_state_machine

LAB_DIR = Path(__file__).parent
PYTHON_EXE = sys.executable


@pytest.fixture(autouse=True)
def clean_environment():
    """Reset the world and lab state before each test."""
    world.reset(stock=5)
    EventHistory().reset()
    status_db = LAB_DIR / "status_table.json"
    status_db.unlink(missing_ok=True)
    checkpoints = LAB_DIR / "checkpoints.json"
    checkpoints.unlink(missing_ok=True)
    yield
    world.reset(stock=5)
    EventHistory().reset()
    status_db.unlink(missing_ok=True)
    checkpoints.unlink(missing_ok=True)


def run_subproc(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=LAB_DIR,
        capture_output=True,
        text=True,
    )


# ==============================================================================
# 1. Checkout v1 & v2 Tests
# ==============================================================================

def test_checkout_v2_happy_path():
    checkout_v2("ORD-TEST-1", crash_at=None)
    data = world._load()
    assert len(data["orders"]) == 1
    assert len(data["charges"]) == 1
    assert len(data["reservations"]) == 1
    assert len(data["emails"]) == 1
    assert world.find_problems(data) == []


def test_checkout_v2_recovery_after_charge_saved():
    # 1. Run with crash in subprocess
    proc = run_subproc([PYTHON_EXE, "checkout_v2.py", "ORD-TEST-2", "--crash-at", "after_charge_saved"])
    assert proc.returncode == 1
    assert "CRASH at 'after_charge_saved'" in proc.stdout

    # 2. Restart in subprocess
    proc2 = run_subproc([PYTHON_EXE, "checkout_v2.py", "ORD-TEST-2"])
    assert proc2.returncode == 0

    data = world._load()
    assert len(data["charges"]) == 1
    assert len(data["reservations"]) == 1
    assert len(data["emails"]) == 1
    assert world.find_problems(data) == []


def test_checkout_v2_idempotency_closes_after_charge_gap():
    # 1. Crashes after bank charges, but before state is saved!
    proc = run_subproc([PYTHON_EXE, "checkout_v2.py", "ORD-TEST-3", "--crash-at", "after_charge"])
    assert proc.returncode == 1
    assert "CRASH at 'after_charge'" in proc.stdout

    # 2. Restart: because of idempotency_key=order_id, no duplicate charge!
    proc2 = run_subproc([PYTHON_EXE, "checkout_v2.py", "ORD-TEST-3"])
    assert proc2.returncode == 0

    data = world._load()
    assert len(data["charges"]) == 1
    assert len(data["reservations"]) == 1
    assert world.find_problems(data) == []


# ==============================================================================
# 2. Usual Fixes Tests
# ==============================================================================

def test_usual_fixes_retries_handles_transient_network():
    run_retries_demo("ORD-TEST-RETRY", fail_network=True, crash_at=None)
    data = world._load()
    assert len(data["charges"]) == 1
    assert world.find_problems(data) == []


def test_usual_fixes_state_machine_gap_leads_to_duplicate():
    # State machine crashes in subprocess after bank charge before DB row update
    proc = run_subproc([PYTHON_EXE, "usual_fixes.py", "state-machine", "ORD-TEST-SM", "--crash-at", "after_charge_before_db"])
    assert proc.returncode == 1

    # Poller recovers order, but unaware of past charge, charges again!
    recover_state_machine()
    data = world._load()
    assert len(data["charges"]) == 2
    problems = world.find_problems(data)
    assert any("charged 2 times" in p for p in problems)


def test_usual_fixes_queue_visibility_timeout_duplicate():
    # Slow worker causes visibility timeout to expire -> duplicate charge
    run_queue_pipeline("ORD-TEST-QUEUE", slow_worker=True)
    data = world._load()
    assert len(data["charges"]) == 2
    problems = world.find_problems(data)
    assert any("charged 2 times" in p for p in problems)


# ==============================================================================
# 3. Durable Execution Tests
# ==============================================================================

def test_durable_execution_replay():
    # 1. Run with crash after charge in subprocess
    proc = run_subproc([PYTHON_EXE, "durable_execution.py", "run", "ORD-TEST-DUR", "--crash-at", "after_charge"])
    assert proc.returncode == 1

    # 2. Resume workflow in subprocess: replays create_order, retries charge with key, finishes
    proc2 = run_subproc([PYTHON_EXE, "durable_execution.py", "run", "ORD-TEST-DUR"])
    assert proc2.returncode == 0
    assert "[REPLAY] Activity 'create_order'" in proc2.stdout

    data = world._load()
    assert len(data["orders"]) == 1
    assert len(data["charges"]) == 1
    assert len(data["reservations"]) == 1
    assert len(data["emails"]) == 1
    assert world.find_problems(data) == []


# ==============================================================================
# 4. The Cast Tests
# ==============================================================================

def test_the_cast_execution_flow():
    service = TemporalService()
    task_queue = "test-queue"

    worker = Worker(service, task_queue)
    worker.register_activity("create_order", world.create_order)
    worker.register_activity("charge_card", world.charge_card)
    worker.register_activity("reserve_inventory", world.reserve_inventory)
    worker.register_activity("send_email", world.send_email)

    client = Client(service)
    client.start_checkout("ORD-TEST-CAST", task_queue)

    # Worker polls and executes tasks
    step = 0
    while client.get_status("ORD-TEST-CAST")["status"] == "RUNNING" and step < 20:
        step += 1
        worker.run_once()

    status = client.get_status("ORD-TEST-CAST")
    assert status["status"] == "COMPLETED"
    assert status["result"]["charge_id"] == "ch_1"

    data = world._load()
    assert len(data["orders"]) == 1
    assert len(data["charges"]) == 1
    assert world.find_problems(data) == []


def test_the_cast_zero_workers_queue_buffering():
    service = TemporalService()
    task_queue = "test-queue-zero"
    client = Client(service)

    # Client starts workflow with 0 workers
    client.start_checkout("ORD-TEST-ZERO", task_queue)
    status = client.get_status("ORD-TEST-ZERO")
    assert status["status"] == "RUNNING"
    assert len(service.queues[task_queue].workflow_tasks) == 1

    # Worker comes online later
    worker = Worker(service, task_queue)
    worker.register_activity("create_order", world.create_order)
    worker.register_activity("charge_card", world.charge_card)
    worker.register_activity("reserve_inventory", world.reserve_inventory)
    worker.register_activity("send_email", world.send_email)

    step = 0
    while client.get_status("ORD-TEST-ZERO")["status"] == "RUNNING" and step < 20:
        step += 1
        worker.run_once()

    assert client.get_status("ORD-TEST-ZERO")["status"] == "COMPLETED"
