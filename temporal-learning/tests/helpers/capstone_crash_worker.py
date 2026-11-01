"""All capstone Workers in one process, which dies when a chosen Activity commits.

python -m tests.helpers.capstone_crash_worker <address> <prefix> <data dir> <activity>
"""

import asyncio
import sys
from pathlib import Path

from temporalio.client import Client

from activities.commerce import Fault
from capstone.activities import CapstoneActivities
from capstone.worker import build_worker
from shared.ledger import Ledger
from shared.object_store import ObjectStore


async def main(address: str, prefix: str, data_dir: str, activity_name: str) -> None:
    client = await Client.connect(address)
    activities = CapstoneActivities(
        ObjectStore(Path(data_dir) / "store"),
        Ledger(Path(data_dir) / "ledger.db"),
        {activity_name: Fault("crash", 1)},
    )
    workers = [build_worker(client, role, activities, prefix) for role in ("orchestration", "io", "data")]
    await asyncio.gather(*(worker.run() for worker in workers))


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:5]))
