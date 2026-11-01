"""A cancel that lands mid-Activity arrives as ActivityError or ChildWorkflowError, not
asyncio.CancelledError. Each test here cancels at exactly that moment and checks that the
cleanup still happened and the run is recorded as cancelled."""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor

import boto3
import pytest
from moto import mock_aws
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import is_cancelled_exception
from temporalio.worker import Worker

from activities.aws_pipeline import AwsPipelineActivities, AwsResources, PipelineRequest, RawObject
from activities.commerce import CommerceActivities
from activities.data_pipeline import DataActivities
from shared.models import ChargeRequest, Order
from shared.object_store import ObjectStore
from shared.payloads import DatasetRef
from workflows.aws_pipeline import DurablePipelineWorkflow
from workflows.data_pipeline import DataPipelineWorkflow, PipelineInput
from workflows.fulfillment_platform import ORDERS, OrderFulfillmentWorkflow, PlatformActivities, PlatformInput
from workflows.subscription import SubscriptionInput, SubscriptionWorkflow
from workers.platform import build_worker

pytestmark = pytest.mark.integration


class Gate:
    """A mock Activity reports here when it starts, then waits until it is cancelled."""

    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def hold(self) -> None:
        self.started.set()
        while True:
            activity.heartbeat()
            await asyncio.sleep(0.05)


async def assert_cancelled(handle) -> None:
    with pytest.raises(WorkflowFailureError) as error:
        await asyncio.wait_for(handle.result(), 15)
    assert is_cancelled_exception(error.value.cause)
    assert (await handle.describe()).status.name == "CANCELED"


async def test_subscription_cancelled_during_a_charge(local_env, queue, ledger):
    gate = Gate()

    @activity.defn(name="charge_payment")
    async def stuck_charge(request: ChargeRequest) -> str:
        await gate.hold()
        return "never"

    real = CommerceActivities(ledger)
    async with Worker(
        local_env.client,
        task_queue=queue,
        workflows=[SubscriptionWorkflow],
        activities=[stuck_charge, real.send_email],
        activity_executor=ThreadPoolExecutor(2),
    ):
        # A running Activity pauses time skipping, so this test uses a real, very short month.
        handle = await local_env.client.start_workflow(
            SubscriptionWorkflow.run, SubscriptionInput("c-1", 999, renewal_days=0.00001), id=queue, task_queue=queue
        )
        await asyncio.wait_for(gate.started.wait(), 15)
        await handle.cancel()
        await assert_cancelled(handle)
    assert ledger.count("email") == 1


async def test_aws_pipeline_cancelled_during_processing(local_env, queue, monkeypatch):
    gate = Gate()

    @activity.defn(name="process")
    async def stuck_process(raw: RawObject) -> RawObject:
        await gate.hold()
        return raw

    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(name, "testing")
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="lake")
        s3.put_object(Bucket="lake", Key="source/o.json", Body=json.dumps([{"quantity": 1, "price": 1}]))
        real = AwsPipelineActivities(AwsResources("lake", "t", "topic", "q", "s"))
        async with Worker(
            local_env.client,
            task_queue=queue,
            workflows=[DurablePipelineWorkflow],
            activities=[real.extract_to_s3, stuck_process, real.delete_outputs],
            activity_executor=ThreadPoolExecutor(2),
        ):
            handle = await local_env.client.start_workflow(
                DurablePipelineWorkflow.run,
                PipelineRequest("r-1", "source/o.json", "a@b.c"),
                id=queue,
                task_queue=queue,
            )
            await asyncio.wait_for(gate.started.wait(), 15)
            assert "Contents" in s3.list_objects_v2(Bucket="lake", Prefix="raw/")
            await handle.cancel()
            await assert_cancelled(handle)
        assert "Contents" not in s3.list_objects_v2(Bucket="lake", Prefix="raw/")


async def test_data_pipeline_cancelled_during_quality_check(local_env, queue, tmp_path):
    gate = Gate()

    @activity.defn(name="quality_check")
    async def stuck_check(ref: DatasetRef) -> float:
        await gate.hold()
        return 1.0

    a = DataActivities(ObjectStore(tmp_path))
    async with Worker(
        local_env.client,
        task_queue=queue,
        workflows=[DataPipelineWorkflow],
        activities=[a.extract, a.validate, a.transform, a.load_staging, stuck_check, a.delete_staging],
        activity_executor=ThreadPoolExecutor(4),
    ):
        handle = await local_env.client.start_workflow(
            DataPipelineWorkflow.run, PipelineInput("run-c", ["p1"]), id=queue, task_queue=queue
        )
        await asyncio.wait_for(gate.started.wait(), 15)
        assert a.store.exists("run-c/staging.json")
        await handle.cancel()
        await assert_cancelled(handle)
    assert not a.store.exists("run-c/staging.json")


async def test_data_pipeline_cancelled_during_a_partition(local_env, queue, tmp_path):
    gate = Gate()

    @activity.defn(name="extract")
    async def stuck_extract(request) -> DatasetRef:
        await gate.hold()
        raise AssertionError("unreachable")

    a = DataActivities(ObjectStore(tmp_path))
    async with Worker(
        local_env.client,
        task_queue=queue,
        workflows=[DataPipelineWorkflow],
        activities=[stuck_extract, a.delete_staging],
        activity_executor=ThreadPoolExecutor(2),
    ):
        handle = await local_env.client.start_workflow(
            DataPipelineWorkflow.run, PipelineInput("run-d", ["p1", "p2"]), id=queue, task_queue=queue
        )
        await asyncio.wait_for(gate.started.wait(), 15)
        await handle.cancel()
        await assert_cancelled(handle)


async def test_platform_cancelled_during_shipping_refunds_everything(local_env, queue, ledger):
    gate = Gate()

    @activity.defn(name="ship")
    async def stuck_ship(request) -> str:
        await gate.hold()
        return "never"

    commerce = CommerceActivities(ledger)
    platform = PlatformActivities(commerce)
    prefix = f"{queue}-"
    orders = build_worker(local_env.client, "orders", platform, prefix)
    payments = build_worker(local_env.client, "payments", platform, prefix)
    notifications = build_worker(local_env.client, "notifications", platform, prefix)
    fulfillment = Worker(
        local_env.client,
        task_queue=f"{prefix}platform-fulfillment",
        activities=[commerce.reserve_inventory, commerce.release_inventory, stuck_ship],
        activity_executor=ThreadPoolExecutor(4),
    )
    async with orders, payments, notifications, fulfillment:
        handle = await local_env.client.start_workflow(
            OrderFulfillmentWorkflow.run,
            PlatformInput(Order(queue, "c-1", "keyboard", 1, 5000), prefix),
            id=queue,
            task_queue=prefix + ORDERS,
        )
        await asyncio.wait_for(gate.started.wait(), 15)
        await handle.cancel()
        await assert_cancelled(handle)
    assert (ledger.count("refund"), ledger.count("release")) == (1, 1)
    assert ledger.stock() == 100
