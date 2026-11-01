import os
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from google.protobuf import json_format
from temporalio.api.common.v1 import Payload, Payloads
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowFailureError
from temporalio.worker import Worker

from activities.greetings import GreetingInput, compose_greeting
from scripts.codec_server import build_app
from shared.encryption import ENCODING, EncryptionCodec, encrypted_converter
from workflows.failure_lab import BusinessFailureWorkflow
from workflows.hello import HelloWorkflow

KEY_1, KEY_2 = os.urandom(32), os.urandom(32)
SECRET_NAME = "Very Secret Customer"


def payload(text: bytes = b"hello") -> Payload:
    return Payload(metadata={"encoding": b"json/plain"}, data=text)


def encrypted_client(env, keys, active="key-1") -> Client:
    config = env.client.config()
    config["data_converter"] = encrypted_converter(EncryptionCodec(keys, active))
    return Client(**config)


async def test_codec_round_trip():
    codec = EncryptionCodec({"key-1": KEY_1}, "key-1")
    encoded = await codec.encode([payload()])
    assert b"hello" not in encoded[0].data
    assert (await codec.decode(encoded))[0] == payload()


async def test_same_value_encrypts_differently_each_time():
    codec = EncryptionCodec({"key-1": KEY_1}, "key-1")
    first, second = await codec.encode([payload(), payload()])
    assert first.data != second.data


async def test_key_rotation_keeps_old_data_readable():
    old = await EncryptionCodec({"key-1": KEY_1}, "key-1").encode([payload()])
    rotated = EncryptionCodec({"key-1": KEY_1, "key-2": KEY_2}, "key-2")
    assert (await rotated.decode(old))[0] == payload()
    assert (await rotated.encode([payload()]))[0].metadata["key-id"] == b"key-2"


async def test_unknown_key_is_an_error_not_garbage():
    encoded = await EncryptionCodec({"key-1": KEY_1}, "key-1").encode([payload()])
    with pytest.raises(ValueError):
        await EncryptionCodec({"key-2": KEY_2}, "key-2").decode(encoded)


async def test_tampered_data_is_rejected():
    codec = EncryptionCodec({"key-1": KEY_1}, "key-1")
    encoded = await codec.encode([payload()])
    encoded[0].data = encoded[0].data[:-1] + bytes([encoded[0].data[-1] ^ 1])
    with pytest.raises(Exception):
        await codec.decode(encoded)


async def test_service_stores_only_ciphertext(time_env, queue):
    client = encrypted_client(time_env, {"key-1": KEY_1})
    async with Worker(client, task_queue=queue, workflows=[HelloWorkflow], activities=[compose_greeting]):
        handle = await client.start_workflow(HelloWorkflow.run, GreetingInput(SECRET_NAME), id=queue, task_queue=queue)
        assert SECRET_NAME in (await handle.result()).greeting
    # Read the raw history, as anyone with access to the Service could.
    plain_client = time_env.client.get_workflow_handle(queue)
    raw = b"".join([event.SerializeToString() async for event in plain_client.fetch_history_events()])
    assert SECRET_NAME.encode() not in raw
    assert ENCODING in raw


async def test_failure_messages_are_encrypted_too(time_env, queue):
    client = encrypted_client(time_env, {"key-1": KEY_1})
    async with Worker(client, task_queue=queue, workflows=[BusinessFailureWorkflow]):
        with pytest.raises(WorkflowFailureError) as error:
            await client.execute_workflow(BusinessFailureWorkflow.run, id=queue, task_queue=queue)
    assert "Order violates policy" in str(error.value.cause)
    handle = time_env.client.get_workflow_handle(queue)
    async for event in handle.fetch_history_events():
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
            assert event.workflow_execution_failed_event_attributes.failure.message == "Encoded failure"


async def test_workflow_ids_are_not_encrypted(time_env, queue):
    """So never put a name, email, or account number in a Workflow ID."""
    client = encrypted_client(time_env, {"key-1": KEY_1})
    workflow_id = f"{queue}-customer-jane@example.com"
    async with Worker(client, task_queue=queue, workflows=[HelloWorkflow], activities=[compose_greeting]):
        await client.execute_workflow(HelloWorkflow.run, GreetingInput("x"), id=workflow_id, task_queue=queue)
    assert (await time_env.client.get_workflow_handle(workflow_id).describe()).id == workflow_id


def test_codec_server_requires_a_token_and_decodes():
    codec = EncryptionCodec({"key-1": KEY_1}, "key-1")
    http = TestClient(build_app(codec, "s3cret"))
    body = json_format.MessageToDict(Payloads(payloads=[payload()]))

    assert http.post("/encode", json=body).status_code == 401

    headers = {"Authorization": "Bearer s3cret"}
    encoded = http.post("/encode", json=body, headers=headers).json()
    assert encoded != body
    assert http.post("/decode", json=encoded, headers=headers).json() == body
