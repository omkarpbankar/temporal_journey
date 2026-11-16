"""Checkout that remembers its progress via file checkpoints.

Idea: after each step, save a note saying that step is done. After a crash,
run the program again, and it skips anything already noted.

Crash points:
    after_order, after_order_saved
    after_charge, after_charge_saved
    after_reserve, after_reserve_saved
    after_email, after_email_saved
"""

import argparse
import json

import world

ITEM = "keyboard"
AMOUNT = 100


def load_state(order_id: str) -> dict:
    if not world.CHECKPOINT_FILE.exists():
        return {"done": [], "charge_id": None}
    try:
        data = json.loads(world.CHECKPOINT_FILE.read_text())
        return data.get(order_id, {"done": [], "charge_id": None})
    except (json.JSONDecodeError, OSError):
        return {"done": [], "charge_id": None}


def save_state(order_id: str, state: dict) -> None:
    data = {}
    if world.CHECKPOINT_FILE.exists():
        try:
            data = json.loads(world.CHECKPOINT_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            data = {}
    data[order_id] = state
    world.CHECKPOINT_FILE.write_text(json.dumps(data, indent=2))


def load_done_steps(order_id: str) -> list[str]:
    """Helper keeping backwards compatibility with earlier starter code."""
    return load_state(order_id)["done"]


def checkout(order_id: str, crash_at: str | None) -> None:
    state = load_state(order_id)
    print(f"Starting checkout for {order_id}. Already done: {state['done']}")

    # Step 1: Create Order
    if "order" not in state["done"]:
        world.create_order(order_id, ITEM, AMOUNT)
        print("  1. Order created")
        world.crash_if("after_order", crash_at)
        state["done"].append("order")
        save_state(order_id, state)
        world.crash_if("after_order_saved", crash_at)

    # Step 2: Charge Card
    if "charge" not in state["done"]:
        # Passing order_id as idempotency_key closes the gap where the process crashes
        # right after the bank commits the charge but before the checkpoint is written.
        charge_id = world.charge_card(order_id, AMOUNT, idempotency_key=order_id)
        print(f"  2. Card charged ({charge_id})")
        world.crash_if("after_charge", crash_at)
        state["done"].append("charge")
        state["charge_id"] = charge_id
        save_state(order_id, state)
        world.crash_if("after_charge_saved", crash_at)

    # Step 3: Reserve Inventory
    if "reserve" not in state["done"]:
        world.reserve_inventory(order_id, ITEM)
        print("  3. Inventory reserved")
        world.crash_if("after_reserve", crash_at)
        state["done"].append("reserve")
        save_state(order_id, state)
        world.crash_if("after_reserve_saved", crash_at)

    # Step 4: Send Confirmation Email
    if "email" not in state["done"]:
        charge_id = state.get("charge_id")
        world.send_email(order_id, f"Thanks! Your {ITEM} is on its way. Receipt: {charge_id}")
        print("  4. Confirmation sent")
        world.crash_if("after_email", crash_at)
        state["done"].append("email")
        save_state(order_id, state)
        world.crash_if("after_email_saved", crash_at)

    print("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("order_id")
    parser.add_argument("--crash-at")
    args = parser.parse_args()
    checkout(args.order_id, args.crash_at)
