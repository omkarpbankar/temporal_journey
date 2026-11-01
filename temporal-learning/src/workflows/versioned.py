"""Phase 11: the same Workflow at five points in its life.

Every class shares one Workflow Type name, because that is what a code change looks
like to Temporal: a new body under the old name. A real project has one class that
changes over time. They sit side by side here so tests can compare them.
"""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow

NAME = "ShippingWorkflow"
TIMEOUT = dict(start_to_close_timeout=timedelta(seconds=10))


@activity.defn
async def step_a() -> str:
    return "a"


@activity.defn
async def step_b() -> str:
    return "b"


@activity.defn
async def step_c() -> str:
    return "c"


@workflow.defn(name=NAME)
class ShippingV1:
    """The original."""

    @workflow.run
    async def run(self, delay_seconds: float) -> list[str]:
        steps = [await workflow.execute_activity(step_a, **TIMEOUT)]
        await asyncio.sleep(delay_seconds)
        steps.append(await workflow.execute_activity(step_b, **TIMEOUT))
        return steps


@workflow.defn(name=NAME)
class ShippingV2Unsafe:
    """step_c was inserted with no protection. In-flight runs cannot replay this."""

    @workflow.run
    async def run(self, delay_seconds: float) -> list[str]:
        steps = [await workflow.execute_activity(step_a, **TIMEOUT)]
        steps.append(await workflow.execute_activity(step_c, **TIMEOUT))
        await asyncio.sleep(delay_seconds)
        steps.append(await workflow.execute_activity(step_b, **TIMEOUT))
        return steps


@workflow.defn(name=NAME)
class ShippingV3Patched:
    """Old runs skip step_c, new runs do it, and both replay correctly."""

    @workflow.run
    async def run(self, delay_seconds: float) -> list[str]:
        steps = [await workflow.execute_activity(step_a, **TIMEOUT)]
        if workflow.patched("add-step-c"):
            steps.append(await workflow.execute_activity(step_c, **TIMEOUT))
        await asyncio.sleep(delay_seconds)
        steps.append(await workflow.execute_activity(step_b, **TIMEOUT))
        return steps


@workflow.defn(name=NAME)
class ShippingV4Deprecated:
    """Deploy this once no pre-patch runs remain. The old branch is gone.

    deprecate_patch must sit exactly where patched() sat. Moving it up, even by
    one line, is itself a nondeterministic change.
    """

    @workflow.run
    async def run(self, delay_seconds: float) -> list[str]:
        steps = [await workflow.execute_activity(step_a, **TIMEOUT)]
        workflow.deprecate_patch("add-step-c")
        steps.append(await workflow.execute_activity(step_c, **TIMEOUT))
        await asyncio.sleep(delay_seconds)
        steps.append(await workflow.execute_activity(step_b, **TIMEOUT))
        return steps


@workflow.defn(name=NAME)
class ShippingV5Clean:
    """Deploy this once no run depends on the patch marker. The code is clean again."""

    @workflow.run
    async def run(self, delay_seconds: float) -> list[str]:
        steps = [await workflow.execute_activity(step_a, **TIMEOUT)]
        steps.append(await workflow.execute_activity(step_c, **TIMEOUT))
        await asyncio.sleep(delay_seconds)
        steps.append(await workflow.execute_activity(step_b, **TIMEOUT))
        return steps
