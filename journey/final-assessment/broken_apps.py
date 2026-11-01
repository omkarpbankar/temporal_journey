"""Part B: five broken applications. Each is a mistake made while building this course.

Find the bug in each, describe the production symptom, and fix it.
This file is for reading. It isn't meant to be run.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import SearchAttributeKey
from temporalio.exceptions import ActivityError


# App 1: customers who cancel are sometimes not refunded.
@workflow.defn
class App1:
    @workflow.run
    async def run(self, order) -> None:
        await workflow.execute_activity("charge", order, start_to_close_timeout=timedelta(seconds=30))
        try:
            await workflow.execute_activity("ship", order, start_to_close_timeout=timedelta(hours=1))
        except asyncio.CancelledError:
            await workflow.execute_activity("refund", order, start_to_close_timeout=timedelta(seconds=30))
            raise


# App 2: every order is stuck straight after this deploy, with no failed Workflows.
STATE = SearchAttributeKey.for_keyword("OrderStage")


@workflow.defn
class App2:
    @workflow.run
    async def run(self, order) -> None:
        workflow.upsert_search_attributes([STATE.value_set("STARTED")])
        await workflow.execute_activity("process", order, start_to_close_timeout=timedelta(seconds=30))


# App 3: this is step 2 of removing a patch. Before it, the code was:
#     await workflow.execute_activity("step_a", ...)
#     if workflow.patched("add-c"):
#         await workflow.execute_activity("step_c", ...)
#     await workflow.execute_activity("step_b", ...)
# All pre-patch runs are finished. Live runs fail after this deploy.
@workflow.defn
class App3:
    @workflow.run
    async def run(self) -> None:
        workflow.deprecate_patch("add-c")
        await workflow.execute_activity("step_a", start_to_close_timeout=timedelta(seconds=30))
        await workflow.execute_activity("step_c", start_to_close_timeout=timedelta(seconds=30))
        await workflow.execute_activity("step_b", start_to_close_timeout=timedelta(seconds=30))


# App 4: some orders fail with "Workflow execution already started", but only sometimes.
@workflow.defn
class App4:
    @workflow.run
    async def run(self, order) -> None:
        await workflow.start_child_workflow(
            "NotifyWorkflow",
            order,
            id=f"notify-{order.customer_id}",
            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
        )


# App 5: a cancel arrives while the charge runs, and this subscription is marked as lapsed.
@workflow.defn
class App5:
    @workflow.run
    async def run(self, customer) -> str:
        try:
            await workflow.execute_activity("charge", customer, start_to_close_timeout=timedelta(seconds=30))
        except ActivityError:
            await workflow.execute_activity("mark_lapsed", customer, start_to_close_timeout=timedelta(seconds=30))
            return "lapsed"
        return "active"
