"""YOUR EXERCISE: checkout that remembers its progress.

Idea: after each step, save a note saying that step is done. After a crash,
run the program again, and it skips anything already noted.

Step 1 is done for you. Finish steps 2, 3, and 4 using the same pattern.

Run it:
    python checkout_v2.py ORD-3 --crash-at after_charge
    python checkout_v2.py ORD-3

Crash points to include:
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


def load_done_steps(order_id: str) -> list[str]:
    if not world.CHECKPOINT_FILE.exists():
        return []
    return json.loads(world.CHECKPOINT_FILE.read_text()).get(order_id, [])


def mark_done(order_id: str, step: str) -> None:
    data = {}
    if world.CHECKPOINT_FILE.exists():
        data = json.loads(world.CHECKPOINT_FILE.read_text())
    data.setdefault(order_id, []).append(step)
    world.CHECKPOINT_FILE.write_text(json.dumps(data, indent=2))


def checkout(order_id: str, crash_at: str | None) -> None:
    done = load_done_steps(order_id)
    print(f"Starting checkout for {order_id}. Already done: {done}")

    if "order" not in done:
        world.create_order(order_id, ITEM, AMOUNT)
        print("  1. Order created")
        world.crash_if("after_order", crash_at)
        mark_done(order_id, "order")
        world.crash_if("after_order_saved", crash_at)

    # TODO: step 2, charge the card
    # TODO: step 3, reserve inventory
    # TODO: step 4, send the email. It needs a receipt number.
    #       Think: if the charge happened in an earlier run, where do you
    #       get the charge ID from now?
    raise NotImplementedError("Your turn: finish steps 2, 3, and 4")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("order_id")
    parser.add_argument("--crash-at")
    args = parser.parse_args()
    checkout(args.order_id, args.crash_at)
