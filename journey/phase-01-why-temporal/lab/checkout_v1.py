"""Checkout, written the normal way: four steps in a row.

Run it:
    python checkout_v1.py ORD-1
    python checkout_v1.py ORD-2 --crash-at after_charge

Crash points: after_order, after_charge, after_reserve, after_email
"""

import argparse

import world

ITEM = "keyboard"
AMOUNT = 100


def checkout(order_id: str, crash_at: str | None) -> None:
    print(f"Starting checkout for {order_id}")

    world.create_order(order_id, ITEM, AMOUNT)
    print("  1. Order created")
    world.crash_if("after_order", crash_at)

    charge_id = world.charge_card(order_id, AMOUNT)
    print(f"  2. Card charged ({charge_id})")
    world.crash_if("after_charge", crash_at)

    world.reserve_inventory(order_id, ITEM)
    print("  3. Inventory reserved")
    world.crash_if("after_reserve", crash_at)

    world.send_email(order_id, f"Thanks! Your {ITEM} is on its way. Receipt: {charge_id}")
    print("  4. Confirmation sent")
    world.crash_if("after_email", crash_at)

    print("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("order_id")
    parser.add_argument("--crash-at")
    args = parser.parse_args()
    checkout(args.order_id, args.crash_at)
