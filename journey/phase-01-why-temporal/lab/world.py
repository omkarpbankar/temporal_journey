"""A pretend outside world: an order database, a payment provider, a warehouse, and email.

These stand in for real systems your program talks to. They are saved in
world.json, so like a real bank, they remember what happened even if your
program dies.

Commands:
    python world.py show             See what the outside world looks like
    python world.py reset            Start fresh
    python world.py reset --stock 0  Start fresh with an empty warehouse
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path

WORLD_FILE = Path(__file__).parent / "world.json"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoints.json"
DEFAULT_STOCK = 5


class OutOfStock(Exception):
    """The warehouse has nothing left to reserve."""


def _load() -> dict:
    if not WORLD_FILE.exists():
        reset()
    return json.loads(WORLD_FILE.read_text())


def _save(data: dict) -> None:
    WORLD_FILE.write_text(json.dumps(data, indent=2))


def reset(stock: int = DEFAULT_STOCK) -> None:
    CHECKPOINT_FILE.unlink(missing_ok=True)
    _save({"stock": stock, "orders": [], "charges": [], "reservations": [], "emails": []})


def create_order(order_id: str, item: str, amount: int) -> None:
    data = _load()
    data["orders"].append({"order_id": order_id, "item": item, "amount": amount})
    _save(data)


def charge_card(order_id: str, amount: int, idempotency_key: str | None = None) -> str:
    """Take money from the customer and return a charge ID."""
    data = _load()
    if idempotency_key is not None:
        for charge in data["charges"]:
            if charge["idempotency_key"] == idempotency_key:
                return charge["charge_id"]

    charge_id = f"ch_{len(data['charges']) + 1}"
    data["charges"].append(
        {
            "charge_id": charge_id,
            "order_id": order_id,
            "amount": amount,
            "idempotency_key": idempotency_key,
        }
    )
    _save(data)
    return charge_id


def reserve_inventory(order_id: str, item: str) -> None:
    data = _load()
    if data["stock"] <= 0:
        raise OutOfStock(f"no {item} left for {order_id}")
    data["stock"] -= 1
    data["reservations"].append({"order_id": order_id, "item": item})
    _save(data)


def send_email(order_id: str, message: str) -> None:
    data = _load()
    data["emails"].append({"order_id": order_id, "message": message})
    _save(data)


def crash_if(point: str, crash_at: str | None) -> None:
    """Kill this program instantly if we have reached the chosen crash point.

    os._exit stops Python on the spot, like pulling the power cord. sys.exit
    would not be a fair test, because it raises an exception that still lets
    `finally` blocks run. A real power cut gives you no such courtesy.
    """
    if point == crash_at:
        print(f"\n*** CRASH at '{point}': the program is gone. ***", flush=True)
        os._exit(1)


def find_problems(data: dict) -> list[str]:
    orders = Counter(row["order_id"] for row in data["orders"])
    charges = Counter(row["order_id"] for row in data["charges"])
    reservations = Counter(row["order_id"] for row in data["reservations"])
    emails = Counter(row["order_id"] for row in data["emails"])

    problems = []
    for order_id in orders:
        for label, counts in [
            ("created", orders),
            ("charged", charges),
            ("reserved", reservations),
            ("emailed", emails),
        ]:
            if counts[order_id] > 1:
                problems.append(f"{order_id} was {label} {counts[order_id]} times")
        if charges[order_id] and not reservations[order_id]:
            problems.append(f"{order_id} paid, but no item is reserved")
        if reservations[order_id] and not emails[order_id]:
            problems.append(f"{order_id} is reserved, but the customer was never told")
        if not charges[order_id]:
            problems.append(f"{order_id} exists, but was never paid for")
    return problems


def show() -> None:
    data = _load()
    print(f"Stock left:   {data['stock']}")
    for name in ("orders", "charges", "reservations", "emails"):
        rows = data[name]
        print(f"{name.capitalize() + ':':<13} {len(rows)}")
        for row in rows:
            print(f"    {row}")

    problems = find_problems(data)
    print()
    if problems:
        print("PROBLEMS:")
        for problem in problems:
            print(f"  [!!] {problem}")
    else:
        print("No problems found.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Look at or reset the pretend outside world.")
    parser.add_argument("command", choices=["show", "reset"])
    parser.add_argument("--stock", type=int, default=DEFAULT_STOCK)
    args = parser.parse_args()

    if args.command == "reset":
        reset(args.stock)
        print(f"World reset. Stock: {args.stock}")
    else:
        show()


if __name__ == "__main__":
    main()
