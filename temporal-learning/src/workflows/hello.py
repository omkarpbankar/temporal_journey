import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.greetings import GreetingInput, compose_greeting

TASK_QUEUE = "hello"


@dataclass
class GreetingResult:
    greeting: str
    workflow_id: str


@workflow.defn
class HelloWorkflow:
    @workflow.run
    async def run(self, input: GreetingInput) -> GreetingResult:
        greeting = await workflow.execute_activity(
            compose_greeting,
            input,
            start_to_close_timeout=timedelta(seconds=10),
        )
        return GreetingResult(greeting=greeting, workflow_id=workflow.info().workflow_id)


@workflow.defn
class SlowHelloWorkflow:
    """Two Activities with a timer between them, so you have time to kill the Worker."""

    @workflow.run
    async def run(self, input: GreetingInput) -> list[str]:
        first = await workflow.execute_activity(
            compose_greeting, input, start_to_close_timeout=timedelta(seconds=10)
        )
        await asyncio.sleep(30)
        second = await workflow.execute_activity(
            compose_greeting,
            GreetingInput(input.name, "es"),
            start_to_close_timeout=timedelta(seconds=10),
        )
        return [first, second]
