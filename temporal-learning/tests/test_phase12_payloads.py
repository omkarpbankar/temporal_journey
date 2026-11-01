import dataclasses
from concurrent.futures import ThreadPoolExecutor

import pytest
import temporalio.converter
from temporalio.worker import Worker

from shared.object_store import ObjectStore
from shared.payloads import CustomerRef, JobState, JobStatus, OrderLine, OrderPayload
from workflows.payload_lab import PassByReferenceWorkflow, PassByValueWorkflow, PayloadActivities

converter = temporalio.converter.default().payload_converter


def round_trip(value, target):
    return converter.from_payloads(converter.to_payloads([value]), [target])[0]


def test_nested_dataclasses_survive_serialization():
    order = OrderPayload("o-1", CustomerRef("c-1"), [OrderLine("sku", 2, 500)])
    assert round_trip(order, OrderPayload) == order


def test_enums_survive_serialization():
    assert round_trip(JobStatus("j-1", JobState.RUNNING, 50), JobStatus).state is JobState.RUNNING


def test_type_hints_decide_what_you_get_back():
    order = OrderPayload("o-1", CustomerRef("c-1"), [])
    assert isinstance(converter.from_payloads(converter.to_payloads([order]))[0], dict)


def test_adding_a_field_with_a_default_is_backward_compatible():
    @dataclasses.dataclass
    class OldOrder:
        order_id: str
        customer: CustomerRef
        lines: list[OrderLine]

    assert round_trip(OldOrder("o-1", CustomerRef("c-1"), []), OrderPayload).currency == "USD"


def test_adding_a_required_field_breaks_old_data():
    @dataclasses.dataclass
    class OldOrder:
        order_id: str

    with pytest.raises(Exception):
        round_trip(OldOrder("o-1"), OrderPayload)


def test_store_detects_changed_data(tmp_path):
    store = ObjectStore(tmp_path)
    ref = store.put_rows("a.json", [{"x": 1}])
    (tmp_path / "a.json").write_text("[]")
    with pytest.raises(ValueError):
        store.get_rows(ref)


@pytest.mark.integration
async def test_reference_keeps_history_small(local_env, queue, tmp_path):
    client = local_env.client
    activities = PayloadActivities(ObjectStore(tmp_path))

    async def history_bytes(workflow_class, suffix):
        name = f"{queue}-{suffix}"
        async with Worker(
            client,
            task_queue=name,
            workflows=[workflow_class],
            activities=[
                activities.fetch_inline,
                activities.count_inline,
                activities.fetch_to_store,
                activities.count_from_store,
            ],
            activity_executor=ThreadPoolExecutor(4),
        ):
            handle = await client.start_workflow(workflow_class.run, 5000, id=name, task_queue=name)
            assert await handle.result() == 5000
        history = await handle.fetch_history()
        return sum(event.ByteSize() for event in history.events)

    by_value = await history_bytes(PassByValueWorkflow, "value")
    by_reference = await history_bytes(PassByReferenceWorkflow, "reference")
    print(f"OBSERVED history bytes: by value {by_value:,}, by reference {by_reference:,}")
    assert by_reference * 50 < by_value


@pytest.mark.integration
async def test_oversized_payload_is_refused(local_env, queue):
    client = local_env.client
    with pytest.raises(Exception) as error:
        await client.start_workflow(PassByValueWorkflow.run, ["x" * 3_000_000], id=queue, task_queue=queue)
    print("OBSERVED oversized:", type(error.value).__name__, str(error.value)[:160])
