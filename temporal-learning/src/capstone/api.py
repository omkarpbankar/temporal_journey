"""Capstone API: the thin front door. All real state lives in the Workflow.

    uvicorn capstone.api:app --port 8000
"""

import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel
from temporalio.client import Client, RPCError, WorkflowUpdateFailedError
from temporalio.common import WorkflowIDConflictPolicy

from capstone.models import ORCHESTRATION, ProcessingRequest
from capstone.workflows import DataRequestWorkflow
from shared.connection import connect


class SubmitBody(BaseModel):
    request_id: str
    source_name: str
    notify_email: str
    rows: int = 100
    null_fraction: float = 0.0


class DecisionBody(BaseModel):
    approved: bool


class EmailBody(BaseModel):
    email: str


def workflow_id(request_id: str) -> str:
    return f"data-request-{request_id}"


def build_api(client: Client, token: str, queue_prefix: str = "", **request_defaults) -> FastAPI:
    app = FastAPI(title="Data Processing Platform")

    def authorised(authorization: str = Header(default="")) -> None:
        if authorization != f"Bearer {token}":
            raise HTTPException(status_code=401, detail="unauthorised")

    def handle(request_id: str):
        return client.get_workflow_handle_for(DataRequestWorkflow.run, workflow_id(request_id))

    @app.post("/requests", status_code=202, dependencies=[Depends(authorised)])
    async def submit(body: SubmitBody) -> dict:
        # A client retry reuses the same request_id, so USE_EXISTING returns the running
        # Workflow instead of starting a second one. The API itself becomes idempotent.
        started = await client.start_workflow(
            DataRequestWorkflow.run,
            ProcessingRequest(queue_prefix=queue_prefix, **body.model_dump(), **request_defaults),
            id=workflow_id(body.request_id),
            task_queue=queue_prefix + ORCHESTRATION,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        return {"workflow_id": started.id, "run_id": started.result_run_id}

    @app.get("/requests/{request_id}", dependencies=[Depends(authorised)])
    async def status(request_id: str) -> dict:
        try:
            current = await handle(request_id).query(DataRequestWorkflow.status)
        except RPCError as error:
            raise HTTPException(status_code=404, detail="no such request") from error
        return current.__dict__

    @app.post("/requests/{request_id}/decision", status_code=202, dependencies=[Depends(authorised)])
    async def decide(request_id: str, body: DecisionBody) -> dict:
        await handle(request_id).signal(DataRequestWorkflow.decide, body.approved)
        return {"accepted": True}

    @app.patch("/requests/{request_id}/notify-email", dependencies=[Depends(authorised)])
    async def change_email(request_id: str, body: EmailBody) -> dict:
        try:
            previous = await handle(request_id).execute_update(DataRequestWorkflow.change_notify_email, body.email)
        except WorkflowUpdateFailedError as error:
            raise HTTPException(status_code=409, detail=str(error.cause)) from error
        return {"previous": previous}

    @app.delete("/requests/{request_id}", status_code=202, dependencies=[Depends(authorised)])
    async def cancel(request_id: str) -> dict:
        await handle(request_id).cancel()
        return {"cancelling": True}

    return app


@asynccontextmanager
async def _lifespan(app: FastAPI):
    client = await connect()
    app.mount("/", build_api(client, os.environ["CAPSTONE_API_TOKEN"]))
    yield


app = FastAPI(lifespan=_lifespan)
