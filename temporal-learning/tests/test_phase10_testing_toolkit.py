"""Testing techniques that didn't fit neatly into an earlier phase."""

import asyncio
import json
from pathlib import Path

import pytest
from temporalio import activity, workflow
from temporalio.client import WorkflowHistory
from temporalio.exceptions import CancelledError as ActivityCancelled
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Replayer, Worker

from activities.greetings import GreetingInput
from activities.long_running import ExportRequest, export_rows
from workflows.hello import HelloWorkflow, SlowHelloWorkflow

HISTORIES = Path(__file__).parent / "histories"


@activity.defn(name="compose_greeting")
async def fake_greeting(input: GreetingInput) -> str:
    return "mocked"


async def test_mock_an_activity_by_registering_its_name(time_env, queue):
    async with Worker(time_env.client, task_queue=queue, workflows=[HelloWorkflow], activities=[fake_greeting]):
        result = await time_env.client.execute_workflow(
            HelloWorkflow.run, GreetingInput("Asha"), id=queue, task_queue=queue
        )
    assert result.greeting == "mocked"


async def test_activity_sees_cancellation_through_its_heartbeat():
    env = ActivityEnvironment()
    task = asyncio.ensure_future(env.run(export_rows, ExportRequest(rows=1000, seconds_per_row=0.01)))
    await asyncio.sleep(0.05)
    env.cancel()
    with pytest.raises((asyncio.CancelledError, ActivityCancelled)):
        await task


async def test_recording_is_replayable_after_a_json_round_trip(time_env, queue):
    from activities.greetings import compose_greeting

    async with Worker(time_env.client, task_queue=queue, workflows=[HelloWorkflow], activities=[compose_greeting]):
        handle = await time_env.client.start_workflow(
            HelloWorkflow.run, GreetingInput("Asha"), id=queue, task_queue=queue
        )
        await handle.result()
    history = await handle.fetch_history()
    restored = WorkflowHistory.from_json(queue, history.to_json())
    await Replayer(workflows=[HelloWorkflow]).replay_workflow(restored)


@pytest.mark.parametrize("path", sorted(HISTORIES.glob("*.json")), ids=lambda p: p.stem)
async def test_saved_production_history_still_replays(path):
    """Keep real histories in the repo. If a code change breaks one, this test goes red in CI."""
    record = json.loads(path.read_text())
    workflows = {"HelloWorkflow": HelloWorkflow, "SlowHelloWorkflow": SlowHelloWorkflow}
    history = WorkflowHistory.from_json(record["workflow_id"], record["history"])
    await Replayer(workflows=[workflows[record["workflow_type"]]]).replay_workflow(history)
