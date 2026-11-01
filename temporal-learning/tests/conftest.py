import uuid

import pytest
import pytest_asyncio
from temporalio.common import SearchAttributeKey
from temporalio.testing import WorkflowEnvironment

from shared.ledger import Ledger


@pytest_asyncio.fixture(scope="session")
async def time_env():
    """Fast tests: the test server jumps over timers."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


@pytest_asyncio.fixture(scope="session")
async def local_env():
    """Real server, real clock. Needed for visibility, crash, and mid-Activity tests."""
    # A Search Attribute must be registered first. Upserting an unknown one fails the Workflow Task.
    attributes = [SearchAttributeKey.for_keyword("OrderState")]
    async with await WorkflowEnvironment.start_local(search_attributes=attributes) as env:
        yield env


@pytest.fixture
def ledger(tmp_path) -> Ledger:
    return Ledger(tmp_path / "ledger.db")


@pytest.fixture
def queue() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"
