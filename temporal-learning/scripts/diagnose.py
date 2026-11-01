"""Investigate one Workflow.

    python scripts/diagnose.py <workflow id>
"""

import asyncio
import sys

from shared.connection import connect
from shared.diagnose import diagnose


async def main(workflow_id: str) -> None:
    client = await connect()
    report = await diagnose(client.get_workflow_handle(workflow_id))
    print(f"Status:  {report.status}")
    print(f"Verdict: {report.verdict}")
    for line in report.evidence:
        print(f"  - {line}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
