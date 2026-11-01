"""A pretend outside world stored in SQLite, so it survives a Worker crash.

It plays the bank, the warehouse, and the email provider. Tests read it to check
what *really* happened, which is the only honest way to test side effects.
"""

import sqlite3
from pathlib import Path


class OutOfStock(Exception):
    pass


class Ledger:
    def __init__(self, path: str | Path, stock: int = 100) -> None:
        self.path = str(path)
        with self._connect() as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS effects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    key TEXT NOT NULL,
                    detail TEXT NOT NULL DEFAULT '',
                    idempotency_key TEXT UNIQUE
                )"""
            )
            db.execute("CREATE TABLE IF NOT EXISTS stock (quantity INTEGER NOT NULL)")
            if db.execute("SELECT COUNT(*) FROM stock").fetchone()[0] == 0:
                db.execute("INSERT INTO stock VALUES (?)", (stock,))

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=10)

    def apply(self, kind: str, key: str, detail: str = "", idempotency_key: str | None = None) -> str:
        """Record a side effect. With an idempotency key, a repeat returns the first result."""
        with self._connect() as db:
            if idempotency_key is not None:
                row = db.execute(
                    "SELECT id FROM effects WHERE idempotency_key = ?", (idempotency_key,)
                ).fetchone()
                if row:
                    return f"{kind}_{row[0]}"
            cursor = db.execute(
                "INSERT INTO effects (kind, key, detail, idempotency_key) VALUES (?, ?, ?, ?)",
                (kind, key, detail, idempotency_key),
            )
            return f"{kind}_{cursor.lastrowid}"

    def reserve(self, order_id: str, quantity: int, idempotency_key: str | None = None) -> str:
        with self._connect() as db:
            if idempotency_key is not None:
                row = db.execute(
                    "SELECT id FROM effects WHERE idempotency_key = ?", (idempotency_key,)
                ).fetchone()
                if row:
                    return f"reservation_{row[0]}"
            updated = db.execute(
                "UPDATE stock SET quantity = quantity - ? WHERE quantity >= ?", (quantity, quantity)
            ).rowcount
            if not updated:
                raise OutOfStock(f"not enough stock for {order_id}")
            cursor = db.execute(
                "INSERT INTO effects (kind, key, idempotency_key) VALUES ('reservation', ?, ?)",
                (order_id, idempotency_key),
            )
            return f"reservation_{cursor.lastrowid}"

    def release(self, order_id: str, quantity: int, idempotency_key: str | None = None) -> None:
        with self._connect() as db:
            if idempotency_key is not None and db.execute(
                "SELECT 1 FROM effects WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone():
                return
            db.execute("UPDATE stock SET quantity = quantity + ?", (quantity,))
            db.execute(
                "INSERT INTO effects (kind, key, idempotency_key) VALUES ('release', ?, ?)",
                (order_id, idempotency_key),
            )

    def count(self, kind: str, key: str | None = None) -> int:
        query, args = "SELECT COUNT(*) FROM effects WHERE kind = ?", [kind]
        if key is not None:
            query += " AND key = ?"
            args.append(key)
        with self._connect() as db:
            return db.execute(query, args).fetchone()[0]

    def stock(self) -> int:
        with self._connect() as db:
            return db.execute("SELECT quantity FROM stock").fetchone()[0]
