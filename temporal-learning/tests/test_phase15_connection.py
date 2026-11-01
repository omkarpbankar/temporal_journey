"""The same code reaches local, self-hosted, or Cloud. Only configuration changes."""

import pytest
from temporalio.worker import Worker

from activities.greetings import GreetingInput, compose_greeting
from shared.connection import connect, connect_config
from workflows.hello import HelloWorkflow


def test_default_is_local_with_no_settings(monkeypatch):
    for name in ("TEMPORAL_ADDRESS", "TEMPORAL_NAMESPACE", "TEMPORAL_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TEMPORAL_CONFIG_FILE", "does-not-exist.toml")
    assert connect_config()["target_host"] == "localhost:7233"


def test_environment_selects_a_cloud_namespace(monkeypatch):
    monkeypatch.setenv("TEMPORAL_ADDRESS", "my-ns.abc12.tmprl.cloud:7233")
    monkeypatch.setenv("TEMPORAL_NAMESPACE", "my-ns.abc12")
    monkeypatch.setenv("TEMPORAL_API_KEY", "not-a-real-key")
    config = connect_config()
    assert config["target_host"] == "my-ns.abc12.tmprl.cloud:7233"
    assert config["namespace"] == "my-ns.abc12"
    assert config["api_key"] == "not-a-real-key"


def test_toml_profile_selects_staging(monkeypatch, tmp_path):
    file = tmp_path / "temporal.toml"
    file.write_text('[profile.default]\naddress = "staging.example:7233"\nnamespace = "staging"\n')
    for name in ("TEMPORAL_ADDRESS", "TEMPORAL_NAMESPACE", "TEMPORAL_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TEMPORAL_CONFIG_FILE", str(file))
    config = connect_config()
    assert config["target_host"] == "staging.example:7233"
    assert config["namespace"] == "staging"


@pytest.mark.integration
async def test_connect_reaches_a_real_server(local_env, queue, monkeypatch):
    monkeypatch.setenv("TEMPORAL_ADDRESS", local_env.client.service_client.config.target_host)
    client = await connect()
    async with Worker(client, task_queue=queue, workflows=[HelloWorkflow], activities=[compose_greeting]):
        result = await client.execute_workflow(HelloWorkflow.run, GreetingInput("Asha"), id=queue, task_queue=queue)
    assert result.greeting == "Hello, Asha!"
