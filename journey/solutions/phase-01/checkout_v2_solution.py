"""One working answer to the Phase 1 exercise. Try it yourself first.

Copy this over lab/checkout_v2.py to run it. It includes the stretch goal: the
idempotency key that closes the gap checkpoints can't, for the charge.

It is deliberately not perfect. I ran every crash point. after_charge is now safe, but
after_order, after_reserve, and after_email still duplicate, because those world.py
calls accept no key. Every step needs its own protection, which is the lesson.
"""

import argparse
import json

import world

ITEM = "keyboard"
AMOUNT = 100


def load_state(order_id: str) -> dict:
    if not world.CHECKPOINT_FILE.exists():
        return {"done": [], "charge_id": None}
    return json.loads(world.CHECKPOINT_FILE.read_text()).get(order_id, {"done": [], "charge_id": None})


def save_state(order_id: str, state: dict) -> None:
    data = {}
    if world.CHECKPOINT_FILE.exists():
        data = json.loads(world.CHECKPOINT_FILE.read_text())
    data[order_id] = state
    world.CHECKPOINT_FILE.write_text(json.dumps(data, indent=2))


def checkout(order_id: str, crash_at: str | None) -> None:
    state = load_state(order_id)
    print(f"Starting checkout for {order_id}. Already done: {state['done']}")

    if "order" not in state["done"]:
        world.create_order(order_id, ITEM, AMOUNT)
        print("  1. Order created")
        world.crash_if("after_order", crash_at)
        state["done"].append("order")
        save_state(order_id, state)
        world.crash_if("after_order_saved", crash_at)

    if "charge" not in state["done"]:
        # The key closes the gap: a repeat after a crash returns the first charge.
        charge_id = world.charge_card(order_id, AMOUNT, idempotency_key=order_id)
        print(f"  2. Card charged ({charge_id})")
        world.crash_if("after_charge", crash_at)
        state["done"].append("charge")
        # charge_id was a local variable, so it must be saved by hand to survive.
        state["charge_id"] = charge_id
        save_state(order_id, state)
        world.crash_if("after_charge_saved", crash_at)

    if "reserve" not in state["done"]:
        world.reserve_inventory(order_id, ITEM)
        print("  3. Inventory reserved")
        world.crash_if("after_reserve", crash_at)
        state["done"].append("reserve")
        save_state(order_id, state)
        world.crash_if("after_reserve_saved", crash_at)

    if "email" not in state["done"]:
        world.send_email(order_id, f"Thanks! Your {ITEM} is on its way. Receipt: {state['charge_id']}")
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
