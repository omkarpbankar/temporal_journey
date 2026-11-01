"""Phase 9: CPU work. Python threads share the GIL, so this needs separate processes."""

import hashlib
import time

from temporalio import activity


@activity.defn
def hash_block(rounds: int) -> str:
    digest = b"seed"
    for _ in range(rounds):
        digest = hashlib.sha256(digest).digest()
    return digest.hex()


@activity.defn
async def slow_call(seconds: float) -> str:
    import asyncio

    await asyncio.sleep(seconds)
    return "done"


@activity.defn
def blocking_call(seconds: float) -> float:
    """Pretend slow third-party SDK. Fine in a thread pool, harmful in async def."""
    start = time.monotonic()
    time.sleep(seconds)
    return time.monotonic() - start
