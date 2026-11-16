"""Lesson 5 Lab: Pull the Plug Failure Harness.

Systematically tests what happens when the Worker disappears at every point in the lifecycle:
1. Normal script (checkout_v1) -> fails with duplicate charges & lost state.
2. Checkpointed script (checkout_v2) -> survives steps, but fails between charge & save unless idempotent.
3. Durable Replay (durable_execution) + Idempotency -> complete resilience with zero duplicated side effects.
4. Lesson 5 Failure Exercise: Network split / late worker completion race.

Run it:
    python pull_the_plug.py matrix
    python pull_the_plug.py late-worker
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import world

LAB_DIR = Path(__file__).parent
PYTHON_EXE = sys.executable
AMOUNT = 100

CRASH_POINTS = [
    ("after_order", "Crash right after creating order"),
    ("after_charge", "Crash right after charging card (BEFORE state/history saved)"),
    ("after_reserve", "Crash right after reserving inventory"),
    ("after_email", "Crash right after sending confirmation email"),
]


def run_command(cmd: list[str]) -> tuple[int, str]:
    proc = subprocess.run(
        cmd,
        cwd=LAB_DIR,
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout + proc.stderr


def test_scenario(script_name: str, crash_point: str, order_id: str) -> list[str]:
    """Runs a script with a crash point, then restarts it, and checks world problems."""
    world.reset()
    checkpoints = LAB_DIR / "checkpoints.json"
    history = LAB_DIR / "history.json"
    checkpoints.unlink(missing_ok=True)
    history.unlink(missing_ok=True)

    # 1. First run with crash
    if script_name == "durable_execution.py":
        run_command([PYTHON_EXE, script_name, "run", order_id, "--crash-at", crash_point])
    else:
        run_command([PYTHON_EXE, script_name, order_id, "--crash-at", crash_point])

    # 2. Restart (recovery attempt)
    if script_name == "durable_execution.py":
        run_command([PYTHON_EXE, script_name, "run", order_id])
    else:
        run_command([PYTHON_EXE, script_name, order_id])

    # 3. Check world state
    data = world._load()
    return world.find_problems(data)


def run_matrix():
    print("\n" + "=" * 90)
    print(" PULL THE PLUG: COMPREHENSIVE CRASH & RECOVERY MATRIX")
    print("=" * 90)
    print(f"{'Crash Point':<18} | {'Normal (v1)':<22} | {'Checkpoints (v2)':<24} | {'Durable Replay':<20}")
    print("-" * 90)

    for pt, desc in CRASH_POINTS:
        # Test v1
        v1_problems = test_scenario("checkout_v1.py", pt, f"ORD-V1-{pt}")
        v1_status = "DOUBLE CHARGE / ERR" if v1_problems else "OK"
        if any("charged 2 times" in p for p in v1_problems):
            v1_status = "CHARGED 2x [FATAL]"
        elif any("created 2 times" in p for p in v1_problems):
            v1_status = "ORDER 2x [DUP]"

        # Test v2
        v2_problems = test_scenario("checkout_v2.py", pt, f"ORD-V2-{pt}")
        v2_status = "OK (Safe)" if not v2_problems else "FAIL: " + v2_problems[0]

        # Test durable execution
        dur_problems = test_scenario("durable_execution.py", pt, f"ORD-DUR-{pt}")
        dur_status = "OK (Safe Replay)" if not dur_problems else "FAIL: " + dur_problems[0]

        print(f"{pt:<18} | {v1_status:<22} | {v2_status:<24} | {dur_status:<20}")

    print("-" * 90)
    print("\nKey Takeaways:")
    print("1. Normal execution (v1) blindly reruns everything on restart, creating duplicates at every step.")
    print("2. Checkpoints (v2) skip finished steps, but 'after_charge' is ONLY safe because of the idempotency key.")
    print("3. Durable Replay skips finished activities via Event History, and retries unrecorded activities safely.")
    print("=" * 90 + "\n")


def run_late_worker_simulation():
    """Lesson 5 Failure Exercise:
    Worker 1 network drops for 60s. Start-To-Close is 30s.
    Worker 2 picks up the retry and completes.
    Worker 1 recovers late and tries to report its completion.
    """
    print("\n" + "=" * 80)
    print(" LESSON 5 FAILURE EXERCISE: THE LATE WORKER RACE CONDITION")
    print("=" * 80)
    world.reset()

    order_id = "ORD-LATE-1"
    print(f"Scenario for {order_id}:")
    print("1. Activity 'charge_card' is dispatched to Worker 1.")
    print("2. Worker 1 receives task. Start-To-Close timeout = 30 seconds.")
    print("3. Worker 1 network drops! Process is still running and calls bank.charge_card().")
    
    # Simulating Worker 1's non-idempotent vs idempotent charge:
    charge_1 = world.charge_card(order_id, AMOUNT, idempotency_key=order_id)
    print(f"   Worker 1 charged bank successfully: {charge_1}")

    print("4. 30 seconds pass... Temporal Service Start-To-Close TIMEOUT EXPIRES!")
    print("   Temporal Service marks Worker 1's task TIMED_OUT.")
    print("5. Temporal Service schedules retry for 'charge_card'. Worker 2 picks it up!")
    
    # Worker 2 executes retry
    charge_2 = world.charge_card(order_id, AMOUNT, idempotency_key=order_id)
    print(f"   Worker 2 executes retry with idempotency_key='{order_id}': returns {charge_2}")
    print("   Worker 2 reports success to Service. Service records ACTIVITY_TASK_COMPLETED.")

    print("\n6. Worker 1's network finally recovers after 60 seconds!")
    print("   Worker 1 tries to call RespondActivityTaskCompleted...")
    print("   [TEMPORAL SERVICE RESPONSE] -> NotFound / ActivityTaskAlreadyCompleted Error!")
    print("   The Service REJECTS Worker 1's result because Worker 2 already completed it.")

    print("\n7. Final outside world audit:")
    world.show()
    print("\nCONCLUSION:")
    print("- Temporal rejected the duplicate completion report.")
    print("- BUT the bank would have charged TWICE if not for the idempotency key!")
    print("- Therefore: Any activity that produces external side effects MUST be idempotent.")
    print("=" * 80 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Lesson 5 Pull the Plug Failure Harness")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("matrix")
    subparsers.add_parser("late-worker")

    args = parser.parse_args()

    if args.command == "matrix":
        run_matrix()
    elif args.command == "late-worker":
        run_late_worker_simulation()
    else:
        # Default to matrix
        run_matrix()


if __name__ == "__main__":
    main()
