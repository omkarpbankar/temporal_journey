"""Lesson 2 Lab: The Usual Fixes (and why they fall short).

This file implements and contrasts the common non-Temporal approaches discussed in Lesson 2:
1. In-app retries (@retry)
2. Database-backed state machine (status table + poller)
3. Queue-based pipeline (simulated SQS/RabbitMQ with visibility timeouts)

Run it:
    python usual_fixes.py retries ORD-FIX-1
    python usual_fixes.py retries ORD-FIX-2 --fail-network
    python usual_fixes.py retries ORD-FIX-3 --crash-at in_retry

    python usual_fixes.py state-machine ORD-SM-1
    python usual_fixes.py state-machine ORD-SM-2 --crash-at after_charge_before_db
    python usual_fixes.py state-machine-recover ORD-SM-2

    python usual_fixes.py queue ORD-Q-1
    python usual_fixes.py queue ORD-Q-2 --slow-worker
"""

import argparse
import json
import time
from pathlib import Path

import world

DB_FILE = Path(__file__).parent / "status_table.json"
QUEUE_FILE = Path(__file__).parent / "queues.json"

ITEM = "keyboard"
AMOUNT = 100


# ==============================================================================
# Approach 1: Retries in Application Code
# ==============================================================================

def retry_in_app(max_attempts: int = 3, delay: float = 0.1):
    """Decorator demonstrating in-app retry loop.
    
    Flaw: Retry counters and backoff state live entirely in RAM.
    If the process dies, all retries are lost.
    If the response is lost after execution, retrying creates duplicate charges.
    """
    def decorator(fn):
        def wrapper(*args, **kwargs):
            last_err = None
            for attempt in range(1, max_attempts + 1):
                try:
                    print(f"    [Retry Attempt {attempt}/{max_attempts}] Calling {fn.__name__}...")
                    return fn(*args, **kwargs)
                except Exception as e:
                    last_err = e
                    print(f"    [Retry Attempt {attempt}] Failed with: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
            raise RuntimeError(f"All {max_attempts} attempts failed") from last_err
        return wrapper
    return decorator


def run_retries_demo(order_id: str, fail_network: bool, crash_at: str | None) -> None:
    print(f"\n=== Approach 1: In-App Retries for {order_id} ===")
    world.create_order(order_id, ITEM, AMOUNT)
    print("  1. Order created")

    attempt_counter = 0

    @retry_in_app(max_attempts=3, delay=0.05)
    def charge_with_flaky_network():
        nonlocal attempt_counter
        attempt_counter += 1
        if fail_network and attempt_counter < 3:
            raise ConnectionError("Simulated temporary network blip")
        
        charge_id = world.charge_card(order_id, AMOUNT)
        if crash_at == "in_retry":
            print("  [CRASH] Process killed inside retry block right after charge!")
            world.crash_if("in_retry", crash_at)
        return charge_id

    try:
        charge_id = charge_with_flaky_network()
        print(f"  2. Card charged successfully: {charge_id}")
        world.reserve_inventory(order_id, ITEM)
        print("  3. Inventory reserved")
        world.send_email(order_id, f"Receipt: {charge_id}")
        print("  4. Email sent")
        print("Done!")
    except Exception as e:
        print(f"Checkout aborted due to error: {e}")


# ==============================================================================
# Approach 2: Database-Backed State Machine & Poller
# ==============================================================================

def _load_db() -> dict:
    if not DB_FILE.exists():
        return {}
    try:
        return json.loads(DB_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_db(data: dict) -> None:
    DB_FILE.write_text(json.dumps(data, indent=2))


def update_order_status(order_id: str, status: str, **extra) -> None:
    db = _load_db()
    row = db.setdefault(order_id, {"order_id": order_id, "status": "PENDING", "created_at": time.time()})
    row["status"] = status
    row["updated_at"] = time.time()
    row.update(extra)
    _save_db(db)
    print(f"  [DB STATUS UPDATE] {order_id} -> {status}")


def run_state_machine(order_id: str, crash_at: str | None) -> None:
    """Simulates a classic DB status table approach.
    
    States: CREATED -> CHARGING -> CHARGED -> RESERVING -> RESERVED -> COMPLETED
    
    Flaw: There is an inescapable physical gap between committing to an external API
    and updating the database row. A crash in that window leaves the DB saying 'CHARGING'
    while the bank already took the money.
    """
    print(f"\n=== Approach 2: State Machine for {order_id} ===")
    update_order_status(order_id, "CREATED")
    world.create_order(order_id, ITEM, AMOUNT)

    # Begin charge step
    update_order_status(order_id, "CHARGING")
    charge_id = world.charge_card(order_id, AMOUNT)
    print(f"  Card charged by bank: {charge_id}")

    # THE CRITICAL GAP: External charge succeeded, but DB row hasn't been updated yet!
    world.crash_if("after_charge_before_db", crash_at)

    update_order_status(order_id, "CHARGED", charge_id=charge_id)

    # Begin reserve step
    update_order_status(order_id, "RESERVING")
    world.reserve_inventory(order_id, ITEM)
    update_order_status(order_id, "RESERVED")

    # Begin email step
    update_order_status(order_id, "EMAILING")
    world.send_email(order_id, f"Receipt: {charge_id}")
    update_order_status(order_id, "COMPLETED")
    print("Done!")


def recover_state_machine() -> None:
    """The poller/cron job that looks for stuck orders and tries to recover them."""
    print("\n=== Running Poller / Recovery Worker ===")
    db = _load_db()
    if not db:
        print("No orders in status table.")
        return

    for order_id, row in db.items():
        status = row["status"]
        print(f"Inspecting order {order_id} (current status: {status})...")
        if status in ("CHARGING", "CREATED"):
            print(f"  [!] Order {order_id} was stuck in '{status}'. The poller assumes it failed and retries charge...")
            # Here is the disaster: The bank charged it before the crash, but the poller doesn't know!
            charge_id = world.charge_card(order_id, AMOUNT)
            print(f"  [!] Card charged AGAIN by recovery poller: {charge_id}")
            update_order_status(order_id, "CHARGED", charge_id=charge_id)
            world.reserve_inventory(order_id, ITEM)
            update_order_status(order_id, "RESERVED")
            world.send_email(order_id, f"Receipt: {charge_id}")
            update_order_status(order_id, "COMPLETED")
            print(f"  Order {order_id} recovered by poller.")
        elif status == "COMPLETED":
            print(f"  Order {order_id} is already completed. Nothing to do.")


# ==============================================================================
# Approach 3: Queue Pipeline with Visibility Timeout
# ==============================================================================

def run_queue_pipeline(order_id: str, slow_worker: bool = False) -> None:
    """Simulates message queue processing with a visibility timeout (like SQS).
    
    If Worker 1 takes 0.3s to process due to network/latency, but visibility timeout
    is 0.2s, the queue marks the message available again and Worker 2 picks it up,
    causing duplicate execution!
    """
    print(f"\n=== Approach 3: Queue Pipeline for {order_id} ===")
    print("Step 1: Message 'create_order' published to Queue")
    world.create_order(order_id, ITEM, AMOUNT)

    print("Step 2: Message 'charge_card' picked up by Worker A")
    visibility_timeout = 0.2
    start_time = time.time()

    if slow_worker:
        print("  Worker A is taking extra time (latency / slow 3rd party response)...")
        time.sleep(0.3)  # Exceeds visibility timeout!
        
        # SQS / RabbitMQ visibility timeout fires:
        print("  [!] Queue Visibility Timeout expired! Queue re-delivers message to Worker B!")
        print("  Worker B charges the customer:")
        charge_b = world.charge_card(order_id, AMOUNT)
        print(f"    Worker B charge ID: {charge_b}")

        print("  Worker A finally finishes late and also charges the customer:")
        charge_a = world.charge_card(order_id, AMOUNT)
        print(f"    Worker A charge ID: {charge_a}")
        charge_id = charge_a
    else:
        charge_id = world.charge_card(order_id, AMOUNT)
        print(f"  Worker A charged successfully in {time.time() - start_time:.2f}s: {charge_id}")

    print("Step 3: Message 'reserve_inventory' processed")
    world.reserve_inventory(order_id, ITEM)

    print("Step 4: Message 'send_email' processed")
    world.send_email(order_id, f"Receipt: {charge_id}")
    print("Done!")


# ==============================================================================
# CLI Entrypoint
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Demonstrate Lesson 2: The Usual Fixes")
    subparsers = parser.add_subparsers(dest="command")

    # Retries parser
    p_retry = subparsers.add_parser("retries")
    p_retry.add_argument("order_id")
    p_retry.add_argument("--fail-network", action="store_true")
    p_retry.add_argument("--crash-at")

    # State machine parser
    p_sm = subparsers.add_parser("state-machine")
    p_sm.add_argument("order_id")
    p_sm.add_argument("--crash-at")

    # State machine recover parser
    subparsers.add_parser("state-machine-recover")

    # Queue parser
    p_queue = subparsers.add_parser("queue")
    p_queue.add_argument("order_id")
    p_queue.add_argument("--slow-worker", action="store_true")

    args = parser.parse_args()

    if args.command == "retries":
        run_retries_demo(args.order_id, args.fail_network, args.crash_at)
    elif args.command == "state-machine":
        run_state_machine(args.order_id, args.crash_at)
    elif args.command == "state-machine-recover":
        recover_state_machine()
    elif args.command == "queue":
        run_queue_pipeline(args.order_id, args.slow_worker)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
