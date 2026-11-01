"""Start a local Temporal server with the Web UI, without installing the CLI.

    python scripts/dev_server.py

Server: localhost:7233    Web UI: http://localhost:8233
History is saved to temporal-dev.db, so it survives restarting this script.
Press Ctrl+C to stop.
"""

import asyncio

from temporalio.testing import WorkflowEnvironment


async def main() -> None:
    env = await WorkflowEnvironment.start_local(
        port=7233,
        ui=True,
        ui_port=8233,
        dev_server_database_filename="temporal-dev.db",
    )
    async with env:
        print("Temporal is running. Server: localhost:7233  UI: http://localhost:8233")
        await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")
