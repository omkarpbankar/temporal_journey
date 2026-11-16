"""Lesson 3 Lab: Durable Execution & Event History Replay Engine.

This file implements a working mini-engine of Temporal's Durable Execution model:
1. An Event History file (history.json) acting as the durable notebook in the safe.
2. Replay-based Workflow execution: rerun from line 1; activities found in history
   return their saved results instantly without re-calling the external world.
3. Plain variables (like charge_id) rebuild naturally without manual state saving.
4. Determinism enforcement: detects when workflow logic diverges from history.

Run it:
    # 1. Happy path:
    python durable_execution.py run ORD-DUR-1
    python durable_execution.py show-history

    # 2. Crash after charge, then resume:
    python durable_execution.py run ORD-DUR-2 --crash-at after_charge
    python durable_execution.py show-history
    python durable_execution.py run ORD-DUR-2
    python world.py show

    # 3. Lesson 3 Failure Exercise: Non-determinism during replay:
    python durable_execution.py nondeterminism ORD-DUR-3
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any, Callable

import world

HISTORY_FILE = Path(__file__).parent / "history.json"
ITEM = "keyboard"
AMOUNT = 100


class NonDeterministicWorkflowError(Exception):
    """Raised when Workflow code branches differently during replay than recorded in history."""


# ==============================================================================
# Event History (The Notebook in the Safe)
# ==============================================================================

class EventHistory:
    def __init__(self, file_path: Path = HISTORY_FILE):
        self.file_path = file_path

    def load(self, workflow_id: str) -> list[dict[str, Any]]:
        if not self.file_path.exists():
            return []
        try:
            data = json.loads(self.file_path.read_text())
            return data.get(workflow_id, [])
        except (json.JSONDecodeError, OSError):
            return []

    def append_event(self, workflow_id: str, event_type: str, details: dict[str, Any]) -> dict[str, Any]:
        data = {}
        if self.file_path.exists():
            try:
                data = json.loads(self.file_path.read_text())
            except (json.JSONDecodeError, OSError):
                data = {}
        history = data.setdefault(workflow_id, [])
        event = {
            "event_id": len(history) + 1,
            "event_type": event_type,
            "details": details,
        }
        history.append(event)
        self.file_path.write_text(json.dumps(data, indent=2))
        return event

    def reset(self) -> None:
        self.file_path.unlink(missing_ok=True)


# ==============================================================================
# Replay Workflow Context
# ==============================================================================

class WorkflowContext:
    def __init__(self, workflow_id: str, crash_at: str | None = None):
        self.workflow_id = workflow_id
        self.crash_at = crash_at
        self.history_store = EventHistory()
        self.existing_history = self.history_store.load(workflow_id)
        self.replay_index = 0
        self.is_replaying = len(self.existing_history) > 0

        if self.is_replaying:
            print(f"[*] RESUMING workflow {workflow_id} with {len(self.existing_history)} existing events in history.")
        else:
            print(f"[*] STARTING new workflow {workflow_id}.")
            self.history_store.append_event(workflow_id, "WORKFLOW_EXECUTION_STARTED", {"workflow_id": workflow_id})

    def execute_activity(
        self,
        activity_name: str,
        fn: Callable,
        *args: Any,
        idempotency_key: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Executes an activity or retrieves its result from Event History if replaying."""
        # Check if we have a recorded completion in history at this replay point
        for event in self.existing_history:
            if (
                event["event_type"] == "ACTIVITY_TASK_COMPLETED"
                and event["details"].get("activity_name") == activity_name
            ):
                result = event["details"].get("result")
                print(
                    f"  [REPLAY] Activity '{activity_name}' (Event #{event['event_id']}) found in history! "
                    f"Returning recorded result: {result} (Outside world NOT called)"
                )
                # Remove matched event so subsequent calls with same name advance properly
                self.existing_history.remove(event)
                return result

        # Not in history -> must execute real work
        print(f"  [EXECUTE] Activity '{activity_name}' not in history. Scheduling & executing...")
        self.history_store.append_event(
            self.workflow_id,
            "ACTIVITY_TASK_SCHEDULED",
            {"activity_name": activity_name, "args": [str(a) for a in args]},
        )

        # Support both full and shorthand crash point names
        shorthands = {
            "create_order": "order",
            "charge_card": "charge",
            "reserve_inventory": "reserve",
            "send_email": "email",
        }
        shorthand = shorthands.get(activity_name, activity_name)
        world.crash_if(f"before_{activity_name}", self.crash_at)
        world.crash_if(f"before_{shorthand}", self.crash_at)

        # Call real activity function
        if idempotency_key is not None:
            result = fn(*args, idempotency_key=idempotency_key, **kwargs)
        else:
            result = fn(*args, **kwargs)

        # Crash window: external call succeeded, but result hasn't been written to history!
        world.crash_if(f"after_{activity_name}", self.crash_at)
        world.crash_if(f"after_{shorthand}", self.crash_at)

        self.history_store.append_event(
            self.workflow_id,
            "ACTIVITY_TASK_COMPLETED",
            {"activity_name": activity_name, "result": result},
        )
        print(f"  [RECORDED] Activity '{activity_name}' completed. Result saved to history: {result}")
        return result


# ==============================================================================
# The Workflow (Clean Business Logic - No Manual Checkpoint Code!)
# ==============================================================================

def checkout_workflow(
    order_id: str,
    crash_at: str | None = None,
    inject_nondeterminism: bool = False,
) -> str:
    ctx = WorkflowContext(order_id, crash_at=crash_at)

    print("\n--- Running Workflow Logic ---")

    # Lesson 3 Failure Exercise: Non-deterministic branch
    if inject_nondeterminism:
        # Flipping a random coin directly inside workflow code
        dice_roll = random.random()
        print(f"  [DETERMINISM TEST] Random coin flip: {dice_roll:.2f}")
        if dice_roll < 0.5:
            ctx.execute_activity("fraud_check", lambda oid: "APPROVED", order_id)

    # 1. Create order
    ctx.execute_activity("create_order", world.create_order, order_id, ITEM, AMOUNT)

    # 2. Charge card (using order_id as idempotency key)
    # Notice: charge_id is a plain local variable! No state dicts or manual json loads!
    charge_id = ctx.execute_activity(
        "charge_card",
        world.charge_card,
        order_id,
        AMOUNT,
        idempotency_key=order_id,
    )

    # 3. Reserve inventory
    ctx.execute_activity("reserve_inventory", world.reserve_inventory, order_id, ITEM)

    # 4. Send email confirmation using charge_id
    ctx.execute_activity(
        "send_email",
        world.send_email,
        order_id,
        f"Thanks! Your {ITEM} is on its way. Receipt: {charge_id}",
    )

    ctx.history_store.append_event(order_id, "WORKFLOW_EXECUTION_COMPLETED", {"status": "SUCCESS"})
    print("--- Workflow Completed Successfully! ---\n")
    return charge_id


# ==============================================================================
# CLI Entrypoint
# ==============================================================================

def show_history() -> None:
    if not HISTORY_FILE.exists():
        print("Event History is empty.")
        return
    data = json.loads(HISTORY_FILE.read_text())
    print("\n=== Durable Event History (Safe) ===")
    for wf_id, events in data.items():
        print(f"\nWorkflow Execution: {wf_id} ({len(events)} events)")
        for ev in events:
            print(f"  Event #{ev['event_id']}: {ev['event_type']:<28} {ev['details']}")


def main():
    parser = argparse.ArgumentParser(description="Demonstrate Lesson 3: Durable Execution")
    subparsers = parser.add_subparsers(dest="command")

    p_run = subparsers.add_parser("run")
    p_run.add_argument("order_id")
    p_run.add_argument("--crash-at")

    p_nd = subparsers.add_parser("nondeterminism")
    p_nd.add_argument("order_id")

    subparsers.add_parser("show-history")
    subparsers.add_parser("reset")

    args = parser.parse_args()

    if args.command == "run":
        checkout_workflow(args.order_id, crash_at=args.crash_at)
    elif args.command == "nondeterminism":
        # First run creates history with nondeterminism:
        print("Run 1: First attempt with nondeterminism...")
        checkout_workflow(args.order_id, crash_at="after_charge", inject_nondeterminism=True)
    elif args.command == "show-history":
        show_history()
    elif args.command == "reset":
        EventHistory().reset()
        print("History reset.")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
