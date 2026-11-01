"""Capstone Workers. Run one process per role, as many copies as each role needs.

    python -m capstone.worker --role orchestration
    python -m capstone.worker --role io
    python -m capstone.worker --role data

Set CAPSTONE_KEY_BASE64 to encrypt every payload with the Phase 14 codec.
"""

import argparse
import asyncio
import base64
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio.client import Client
from temporalio.converter import DataConverter
from temporalio.worker import Worker

from capstone.activities import CapstoneActivities
from capstone.models import DATA, IO, ORCHESTRATION
from capstone.workflows import ChunkProcessingWorkflow, DataRequestWorkflow
from shared.connection import connect
from shared.encryption import EncryptionCodec, encrypted_converter
from shared.ledger import Ledger
from shared.object_store import ObjectStore


def build_worker(client: Client, role: str, activities: CapstoneActivities, prefix: str = "") -> Worker:
    settings = {"client": client, "graceful_shutdown_timeout": timedelta(seconds=5)}
    if role == "orchestration":
        return Worker(
            task_queue=prefix + ORCHESTRATION,
            workflows=[DataRequestWorkflow, ChunkProcessingWorkflow],
            **settings,
        )
    if role == "data":
        queue = prefix + DATA
        registered = [activities.process_chunk, activities.merge_chunks, activities.quality_check]
    elif role == "io":
        queue = prefix + IO
        registered = [
            activities.create_record,
            activities.source_ready,
            activities.download_source,
            activities.store_results,
            activities.update_metadata,
            activities.write_audit,
            activities.publish_event,
            activities.notify_user,
            activities.delete_artifacts,
        ]
    else:
        raise ValueError(f"unknown role: {role}")
    return Worker(
        task_queue=queue,
        activities=registered,
        activity_executor=ThreadPoolExecutor(20),
        max_concurrent_activities=20,
        **settings,
    )


def data_converter() -> DataConverter | None:
    key = os.environ.get("CAPSTONE_KEY_BASE64")
    return encrypted_converter(EncryptionCodec({"key-1": base64.b64decode(key)}, "key-1")) if key else None


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, choices=["orchestration", "io", "data"])
    parser.add_argument("--data-dir", default="capstone-data")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    converter = data_converter()
    client = await connect(**({"data_converter": converter} if converter else {}))
    activities = CapstoneActivities(ObjectStore(args.data_dir), Ledger(f"{args.data_dir}-ledger.db"))
    print(f"Capstone {args.role} worker running")
    await build_worker(client, args.role, activities).run()


if __name__ == "__main__":
    asyncio.run(main())
