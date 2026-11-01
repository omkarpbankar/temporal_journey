"""Phase 3 failure lab. Each Bad* Workflow does something Workflow code must not do.

The Good Workflow shows the deterministic replacements. Do not copy the Bad
Workflows into real code.
"""

import asyncio
import random
import sqlite3
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime

from temporalio import workflow


@workflow.defn
class BadRandomWorkflow:
    @workflow.run
    async def run(self) -> float:
        return random.random()


@workflow.defn
class BadTimeWorkflow:
    @workflow.run
    async def run(self) -> str:
        return datetime.now().isoformat()


@workflow.defn
class BadFileWorkflow:
    @workflow.run
    async def run(self) -> str:
        with open("anything.txt", "w") as file:
            file.write("oops")
        return "done"


@workflow.defn
class BadHttpWorkflow:
    @workflow.run
    async def run(self) -> str:
        return urllib.request.urlopen("http://example.invalid").read().decode()


@workflow.defn
class BadDatabaseWorkflow:
    @workflow.run
    async def run(self) -> int:
        return sqlite3.connect(":memory:").execute("SELECT 1").fetchone()[0]


@workflow.defn
class BadBlockingWorkflow:
    @workflow.run
    async def run(self) -> str:
        time.sleep(3)
        return "done"


@dataclass
class GoodValues:
    random_value: float
    now: str
    uuid: str


@workflow.defn
class GoodDeterministicWorkflow:
    """These values are safe: on replay, each call gives the original answer again."""

    @workflow.run
    async def run(self) -> GoodValues:
        await asyncio.sleep(1)
        return GoodValues(
            random_value=workflow.random().random(),
            now=workflow.now().isoformat(),
            uuid=str(workflow.uuid4()),
        )
