"""Phase 4: a long Activity that heartbeats, resumes, and handles cancellation."""

import asyncio
from dataclasses import dataclass

from temporalio import activity


@dataclass
class ExportRequest:
    rows: int
    seconds_per_row: float = 0.01
    fail_at_row: int | None = None


@dataclass
class ExportResult:
    rows_this_attempt: int
    attempt: int
    resumed_from: int


@activity.defn
async def export_rows(request: ExportRequest) -> ExportResult:
    info = activity.info()
    # A retry starts this function from the top. Heartbeat details are how it remembers.
    start = info.heartbeat_details[0] if info.heartbeat_details else 0
    done = 0
    try:
        for row in range(start, request.rows):
            await asyncio.sleep(request.seconds_per_row)
            if row == request.fail_at_row and info.attempt == 1:
                raise RuntimeError(f"connection lost at row {row}")
            done += 1
            activity.heartbeat(row + 1)
    except asyncio.CancelledError:
        # Clean up here. The re-raise is required for the Activity to appear cancelled.
        activity.logger.info("Export cancelled after %s rows", done)
        raise
    return ExportResult(done, info.attempt, start)
