# Capstone: Production-Grade Data Processing Platform

A user submits a data-processing request through an API, and a Temporal Workflow carries it through validation, processing, quality checks, publishing, and notification, surviving every failure along the way.

**The reference build is done, and it's tested under chaos. It's your job to understand it, break it, and defend it.** Reading it doesn't count as finishing the capstone.

## The system

```text
User --> API (FastAPI) --> DataRequestWorkflow
                               |
                               +-- validate request            pure Workflow logic
                               +-- create processing record    io queue
                               +-- wait for the source         durable timers
                               +-- download source             io queue, heartbeat
                               +-- ChunkProcessingWorkflow     child, Continue-As-New
                               |      +-- process_chunk x N    data queue, heartbeat
                               +-- merge and quality check     data queue
                               +-- human approval if low       Signal, with a timeout
                               +-- store results               io queue
                               +-- update metadata             io queue
                               +-- write audit                 patched in later
                               +-- publish event               io queue
                               +-- notify user                 io queue
```

Code: [temporal-learning/src/capstone/](../../temporal-learning/src/capstone/). Tests: [tests/test_capstone.py](../../temporal-learning/tests/test_capstone.py).

## Run it

You need five terminals, each with the venv active and in `temporal-learning`.

```powershell
python scripts/dev_server.py
python -m capstone.worker --role orchestration
python -m capstone.worker --role io
python -m capstone.worker --role data

$env:CAPSTONE_API_TOKEN = "demo"
uvicorn capstone.api:app --port 8000
```

Then try it from a sixth:

```powershell
$h = @{ Authorization = "Bearer demo" }
$body = '{"request_id":"r1","source_name":"sales","notify_email":"ops@example.com","rows":100,"null_fraction":0.5}'
Invoke-RestMethod -Method Post -Uri http://localhost:8000/requests -Headers $h -Body $body -ContentType application/json
Invoke-RestMethod -Uri http://localhost:8000/requests/r1 -Headers $h
Invoke-RestMethod -Method Post -Uri http://localhost:8000/requests/r1/decision -Headers $h -Body '{"approved":true}' -ContentType application/json
```

With `null_fraction` at 0.5, the request pauses at `AWAITING_APPROVAL` until you approve. I ran this flow with real processes and got `AWAITING_APPROVAL`, a `409` for a bad email, then `COMPLETED`, with the results file written.

## Required features

| Feature | Where |
|---|---|
| Python SDK, Workflow, Activities, Worker, Client | `capstone/` |
| Task Queues | `capstone-orchestration`, `capstone-io`, `capstone-data` |
| Retries | `RETRY` in `workflows.py` |
| Timeouts | `options()`, and heartbeat timeouts |
| Heartbeats | `download_source`, `process_chunk` |
| Cancellation | `DELETE /requests/{id}`, with cleanup, tested |
| Signals | `decide` |
| Queries | `status` |
| Updates | `change_notify_email`, with a validator |
| Child Workflows | `ChunkProcessingWorkflow` |
| Timers | `_wait_for_source`, and the approval timeout |
| Continue-As-New | `ChunkProcessingWorkflow`, every `chunks_per_run` chunks |
| Idempotency | Derived keys, in every Activity |
| Failure handling | `run()`: clean up, notify, and re-raise |
| Testing | `tests/test_capstone.py` |
| Observability | `status` Query, `data_requests_completed` metric, `diagnose.py` |
| Versioning | `workflow.patched("add-audit-log")` |
| Deployment | `deploy/` |
| Security | API token, optional payload encryption with `CAPSTONE_KEY_BASE64` |
| Documentation | This file |

## Chaos results

Each failure in your plan's list is injected by a test.

| Injected | Test | Outcome |
|---|---|---|
| Worker crash after commit | `test_worker_process_killed_after_commit` | A real process is killed at 3 steps. Each side effect still happens once |
| Network failure | `test_recovers_from_injected_failure[network]` | Retried, completed |
| API failure | `[api]` | Retried, completed |
| Database failure | `[database]` | Retried, completed |
| Duplicate Activity execution | `[dup-store]`, `[dup-metadata]`, `[dup-notify]` | Side effect happens once |
| Timeout | `[timeout]` | Retried on a new attempt |
| Cancellation | `test_cancellation_cleans_up` | Staged data removed |
| Deployment or version change | `test_deployment_change_is_safe_with_a_patch` | Old run replays. The unpatched change fails |
| Retries exhausted | `test_exhausted_retries_fail_cleanly_and_tell_the_user` | Cleaned up, user told |

## Key decisions

| Decision | Why | Trade-off |
|---|---|---|
| Three queues | CPU and IO scale differently, and a slow upstream can't stall processing | More to deploy |
| Pass references, never rows | A payload limit doesn't constrain data size | Needs an object store and retention planning |
| A child for chunking | Its history can grow and roll over without touching the parent | One more run to trace |
| Client-chosen `request_id` | Retrying the API can't start a second run | Clients must supply one |
| Staging, then publish | No one ever sees unapproved data | One extra copy |
| Approval times out as "rejected" | Safer than publishing unreviewed data | Someone may have to resubmit |

## Runbook

| Symptom | Check | Action |
|---|---|---|
| Requests stay in `DOWNLOADING` | `scripts/diagnose.py data-request-<id>` | `NO_ACTIVITY_WORKER`: restart `io` |
| Requests stay in `PROCESSING` | Diagnose the `-chunks` child too | Usually `data` Workers |
| Many `RETRY_STORM` verdicts | The `last_failure` line | Fix the dependency. Retries resume by themselves |
| `NONDETERMINISM` after a deploy | Run a replay test | Roll back or patch. **Don't terminate** |
| Stuck in `AWAITING_APPROVAL` | Expected | Approve, or wait for the timeout |

## What's not production-ready, on purpose

Knowing this list is part of the capstone. Fixing these is your extension work:

- `Ledger` and `ObjectStore` are local stand-ins for a real database and S3.
- The API token is one shared string. Use your identity provider.
- Workers and the API share a Docker volume, which only works on one host.
- `deploy/` targets a dev server. A real deployment needs TLS and credentials, as in Phases 14 and 15.
- The API exposes only state. Real users need paging and history.
- `source_ready` keeps a counter in memory, which is fine for a lab but wrong in a multi-Worker system.

## Your work

1. **Trace it.** Run it, then read the history in the UI, event by event.
2. **Break it.** Kill Workers by hand at moments of your choosing, and predict each outcome first.
3. **Extend it.** Pick two: replace the Ledger with PostgreSQL, deploy to AWS, add per-tenant queues, or add a report-email step using a patch.
4. **Defend it.** Your mentor will challenge each decision above.

Then, and only then, take the [Final Assessment](../final-assessment/README.md).
