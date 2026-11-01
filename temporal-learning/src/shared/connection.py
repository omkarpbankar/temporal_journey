"""One way to connect, everywhere.

Local development needs no settings. For Temporal Cloud or another environment,
set TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, and TEMPORAL_API_KEY, or use a TOML
profile, and no code changes.
"""

from typing import Any

from temporalio.client import Client
from temporalio.envconfig import ClientConfig


def connect_config() -> dict[str, Any]:
    config: dict[str, Any] = dict(ClientConfig.load_client_connect_config())
    config.setdefault("target_host", "localhost:7233")
    return config


async def connect(**overrides: Any) -> Client:
    return await Client.connect(**{**connect_config(), **overrides})
