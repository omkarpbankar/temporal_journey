# Low-Level Design

This document gives the exact contracts in the code: modules, Workflows, Activities, timeouts, idempotency keys, states, and APIs. Use it as a reference while you work through the phases.

For the big picture, see the [High-Level Design](HLD.md). If this document and the code disagree, the code is right.

## 1. Conventions

These rules apply across `temporal-learning/src/`:

| Rule | Why |
|---|---|
| Workflows import Activities inside `workflow.unsafe.imports_passed_through()` | The sandbox re-imports Workflow files |
| Inputs and outputs are single dataclasses, where it's practical | Adding a field doesn't change the signature |
| New dataclass fields have defaults | Old payloads in history must still decode |
| Blocking I/O uses `def` Activities on a thread pool | A block in `async def` freezes the event loop |
| Activity classes receive dependencies through their constructor | Tests can swap them |
| Idempotency keys are built from business IDs, in the Workflow | They must stay the same across retries |
| Cleanup code uses `is_cancelled_exception` | A cancel takes different forms |
| Queue names can take a `queue_prefix` | Tests share one server without colliding |

## 2. Module map

### `src/workflows/`

| Module | Phase | Contents |
|---|---|---|
| `hello.py` | 2 | `HelloWorkflow`, `SlowHelloWorkflow` |
| `order_basics.py` | 3 | `OrderBasicsWorkflow` |
| `determinism_lab.py` | 3 | Six `Bad*Workflow` classes, `GoodDeterministicWorkflow` |
| `checkout.py` | 4 | `CheckoutWorkflow`, `ExportWorkflow` |
| `failure_lab.py` | 5 | One Workflow per failure shape |
| `interactive_order.py` | 6 | `InteractiveOrderWorkflow` |
| `subscription.py` | 7 | `SubscriptionWorkflow` |
| `composed_order.py` | 8 | Four children, sequential, parallel, and fan-out parents |
| `routed_order.py` | 9 | `RoutedOrderWorkflow`, `BurstWorkflow`, `CpuJobWorkflow` |
| `versioned.py` | 11 | `ShippingV1` to `ShippingV5Clean`, all named `ShippingWorkflow` |
| `payload_lab.py` | 12 | `PassByValueWorkflow`, `PassByReferenceWorkflow` |
| `aws_pipeline.py` | 16 | `DurablePipelineWorkflow` |
| `data_pipeline.py` | 17 | `DataPipelineWorkflow` |
| `patterns.py` | 18 | `Saga`, `BatchWorkflow`, `ReportWorkflow`, `WaiterWorkflow`, `NotifierWorkflow`, `HumanApprovalWorkflow` |
| `fulfillment_platform.py` | 18 | `OrderFulfillmentWorkflow`, `PaymentChildWorkflow`, `PlatformActivities` |
| `internals_lab.py` | 19 | `TracedWorkflow` |

### `src/activities/`

| Module | Contents |
|---|---|
| `greetings.py` | `compose_greeting` |
| `commerce.py` | `CommerceActivities`, and the `Fault` injector |
| `long_running.py` | `export_rows`, with heartbeats |
| `cpu_heavy.py` | `hash_block`, `slow_call`, `blocking_call` |
| `aws_pipeline.py` | `AwsPipelineActivities`, `classify` |
| `data_pipeline.py` | `DataActivities` |

### `src/shared/`

| Module | Purpose |
|---|---|
| `models.py` | Order and checkout dataclasses, and `validate_order` |
| `payloads.py` | Phase 12 payload designs, including `DatasetRef` |
| `ledger.py` | SQLite stand-in for external systems |
| `object_store.py` | Folder stand-in for S3 |
| `connection.py` | `connect()`, configured from the environment |
| `encryption.py` | `EncryptionCodec`, `encrypted_converter` |
| `diagnose.py` | `diagnose()`, which classifies a stuck run |

### Runnable entry points

| Command | Role |
|---|---|
| `python -m workers.hello_worker` | Phase 2 Worker |
| `python -m clients.hello_client NAME [--language X] [--slow]` | Phase 2 Client |
| `python -m workers.observed_worker` | Checkout Worker with metrics on port 9000 |
| `python -m workers.specialized --role ROLE [--concurrency N]` | `orders`, `payments`, `inventory`, `notifications`, `data-processing` |
| `python -m workers.platform --role ROLE` | `orders`, `payments`, `fulfillment`, `notifications` |
| `python -m capstone.worker --role ROLE` | `orchestration`, `io`, `data` |
| `uvicorn capstone.api:app` | Capstone API. Needs `CAPSTONE_API_TOKEN` |

Other Workflows have no standalone Worker. Their tests start a Worker on a unique queue, so the `TASK_QUEUE` constants in those modules are labels only.

## 3. Core data models

### Commerce (`shared/models.py`)

```python
Order(order_id, customer_id, item, quantity, amount_cents, address="1 Main St")
Customer(customer_id, email)
ChargeRequest(order_id, amount_cents, idempotency_key=None)
ReserveRequest(order_id, item, quantity, idempotency_key=None)
EmailRequest(order_id, message, idempotency_key=None)
CheckoutInput(order, use_idempotency=True, max_attempts=5, start_to_close_seconds=5)
CheckoutResult(order_id, status, charge_id=None, reason=None, steps=[])
```

### Data references (`shared/payloads.py`)

```python
DatasetRef(uri, rows, size_bytes, sha256)
```

`ObjectStore.get_rows` raises `ValueError` if the data's hash doesn't match.

### Capstone (`capstone/models.py`)

```python
ProcessingRequest(request_id, source_name, notify_email, rows=100, chunk_size=25,
                  null_fraction=0.0, quality_threshold=0.9, approval_hours=24,
                  chunks_per_run=2, activity_timeout_seconds=30, queue_prefix="")
ChunkRequest(request_id, index, source, start, size)
ChunkJob(request, source, next_index=0, finished=[])
MergeRequest(request_id, chunks)
MetadataUpdate(request_id, output_uri, rows)
Notification(request_id, email, message)
ProcessingStatus(state, notify_email, chunks_done=0, quality=None, error=None)
ProcessingResult(state, output_uri=None, quality=None, rows=0)
```

## 4. Workflow reference

### Timeouts and retries

| Workflow | Start-To-Close | Retry policy | Non-retryable types |
|---|---|---|---|
| `HelloWorkflow` | 10s | Default | None |
| `OrderBasicsWorkflow` | 10s | Default | None |
| `CheckoutWorkflow` | Input, default 5s | 100ms to 1s, input attempts, default 5 | `PaymentDeclined`, `OutOfStock` |
| `ExportWorkflow` | 5m, heartbeat 2s | 100ms initial, unlimited | None |
| `InteractiveOrderWorkflow` | 30s | Default | None |
| `SubscriptionWorkflow` (charge) | 30s | 3 attempts | `PaymentDeclined` |
| `composed_order` steps | 30s | 100ms initial, 3 attempts | None |
| `RoutedOrderWorkflow` | 30s, and Schedule-To-Close 1h for email | Default | None |
| `DurablePipelineWorkflow` | 2m, heartbeat 30s on load | 200ms to 30s, 5 attempts | Raised as non-retryable by `classify` |
| `DataPipelineWorkflow` | 5m | 50ms to 200ms, 2 attempts | `BadSchema` |
| `OrderFulfillmentWorkflow` | 30s | 100ms to 5s, 5 attempts | `PaymentDeclined`, `OutOfStock`, `AddressInvalid` |
| `DataRequestWorkflow` | Request, default 30s | 100ms to 2s, 6 attempts | None declared. `ApplicationError` with `non_retryable=True` stops retries |

The short retry intervals keep the labs fast. Production values are usually longer.

### Message handlers

| Workflow | Signals | Queries | Updates |
|---|---|---|---|
| `OrderBasicsWorkflow` | `mark_warehouse_ready` | `current_stage` | None |
| `InteractiveOrderWorkflow` | `confirm_payment(ref)`, `cancel(reason)` | `status` | `change_address(address)`, with a validator |
| `SubscriptionWorkflow` | `request_cancel`, `payment_method_updated` | `current_state`, `paid` | None |
| `RoutedOrderWorkflow` | None | `current_stage` | None |
| `DurablePipelineWorkflow` | None | `current_stage` | None |
| `DataPipelineWorkflow` | `retry_failed_partitions`, `drop_failed_partitions`, `decide(approved)` | `status` | None |
| `HumanApprovalWorkflow` | `approve(approver)` | None | None |
| `OrderFulfillmentWorkflow` | `submit_review(approved)`, `request_cancel` | `status` | `change_address(address)`, with a validator |
| `DataRequestWorkflow` | `decide(approved)` | `status` | `change_notify_email(email)`, with a validator |

### Child Workflow IDs

| Parent | Child ID | Close policy |
|---|---|---|
| `composed_order` parents | `{order_id}-payment`, `-inventory`, `-fulfillment`, `-notification` | Default terminate, except notification in the parallel parent: `ABANDON` |
| `OrderFulfillmentWorkflow` | `{order_id}-payment` | Default |
| `DataRequestWorkflow` | `{workflow_id}-chunks` | Default |

## 5. State machines

### `InteractiveOrderWorkflow`

```text
ORDER_CREATED -> WAITING_FOR_PAYMENT
                   |-- confirm_payment --> PAYMENT_CONFIRMED -> FULFILLING -> COMPLETED
                   |-- cancel -----------> CANCELLED
                   +-- 24 hours pass ----> PAYMENT_TIMED_OUT
```

`change_address` is rejected in `FULFILLING`, `COMPLETED`, `CANCELLED`, and `PAYMENT_TIMED_OUT`.

### `SubscriptionWorkflow`

```text
      +------------------------------------------------+
      v                                                |
SUBSCRIBED --30 days--> RENEWAL_DUE --charge ok--------+
   |                        |
   | request_cancel         | charge declined
   v                        v
CANCELLED <--cancel-- PAYMENT_FAILED --7 days--> LAPSED
                            |
                            +-- payment_method_updated --> retries the charge

total_periods reached --> COMPLETED
periods_per_run reached, or suggested --> Continue-As-New
```

Carried across Continue-As-New: `customer_id`, `amount_cents`, `total_periods`, `periods_per_run`, `renewal_days`, `periods_paid`, and `charge_ids`.

### `DataPipelineWorkflow`

```text
starting -> processing
              |-- some failed --> awaiting_partition_decision
              |                     |-- retry --> reprocessing --> (back to the check)
              |                     |-- drop ---> continue with what succeeded
              |                     +-- timeout -> result: timed_out
              v
           loading -> quality_check
                         |-- at or above threshold ---------> publishing -> completed
                         +-- below --> awaiting_approval
                                         |-- approved ------> publishing
                                         +-- rejected or timeout --> rejected, staging deleted
```

### `OrderFulfillmentWorkflow`

```text
RECEIVED -> FRAUD_CHECK
              |-- score 0.7 or more --> REVIEW --> REJECTED or CANCELLED
              v                            |
            PAYING <---- approved ---------+
              v
           RESERVING -> SHIPPING -> COMPLETED
              |            |
              +--failure---+--> COMPENSATING -> FAILED or CANCELLED
```

Each state is written to the `OrderState` Search Attribute, which must be registered on the server first.

### `DataRequestWorkflow`

```text
RECEIVED -> CREATING_RECORD -> WAITING_FOR_SOURCE -> DOWNLOADING -> PROCESSING
         -> QUALITY_CHECK
              |-- quality met -----------------------------+
              +-- low --> AWAITING_APPROVAL --approved-----+
                               |                           v
                               +--rejected--> REJECTED   STORING -> PUBLISHING -> COMPLETED

Any failure:  CLEANING_UP -> FAILED
Any cancel:   CANCELLING  -> CANCELLED
```

Invalid requests (`rows` or `chunk_size` not positive) raise `ApplicationError` before any side effect.

## 6. Activity reference

### `CommerceActivities`

| Activity | Input | Effect kind | Non-retryable errors |
|---|---|---|---|
| `create_customer` | `Customer` | `customer` | None |
| `charge_payment` | `ChargeRequest` | `charge` | `PaymentDeclined`, at 1,000,000 cents or more |
| `refund_payment` | `ChargeRequest` | `refund` | None |
| `reserve_inventory` | `ReserveRequest` | `reservation` | `OutOfStock` |
| `release_inventory` | `ReserveRequest` | `release` | None |
| `send_email` | `EmailRequest` | `email` | None |
| `write_database` | `str` | `db_row` | None |
| `call_external_api` | `str` | `api_call` | None |

### Idempotency keys

| Step | Key |
|---|---|
| Charge | `{order_id}-charge` |
| Refund | `{order_id}-refund` |
| Reserve | `{order_id}-reserve` |
| Release | `{order_id}-release` |
| Email | `{order_id}-email`, or a step-specific suffix on the platform |
| Subscription charge | `{customer_id}-period-{n}` |
| Subscription goodbye | `{customer_id}-goodbye` |
| Shipment | `{order_id}-ship` |
| Capstone record | `{request_id}-record` |
| Capstone metadata | `{request_id}-metadata` |
| Capstone event | `{request_id}-event` |
| Capstone notification | `{request_id}-{message}` |
| Capstone audit | `{request_id}-audit` |
| Capstone cleanup | `{request_id}-cleanup` |

Object store writes are idempotent because the key is derived from the request. A retry overwrites identical content.

### The `Fault` injector

`Fault(kind, times=1, seconds=0)` misbehaves on attempts 1 to `times`.

| Kind | Behaviour | Teaches |
|---|---|---|
| `transient` | Fails before the side effect | Retries |
| `permanent` | Non-retryable failure | Failure classification |
| `hang` | Sleeps for `seconds` | Timeouts |
| `lost_response` | Does the side effect, then fails | Duplicate execution |
| `crash` | Does the side effect, then `os._exit(1)` | Worker crash |

It exists only for labs, and production Activities wouldn't have it.

### Capstone Activities (`CapstoneActivities`)

| Activity | Queue | Heartbeat | Output |
|---|---|---|---|
| `create_record` | io | No | Record ID |
| `source_ready` | io | No | `bool` |
| `download_source` | io | Yes | `DatasetRef` |
| `process_chunk` | data | Yes | `DatasetRef` |
| `merge_chunks` | data | No | `DatasetRef` |
| `quality_check` | data | No | `float` |
| `store_results` | io | No | `DatasetRef` |
| `update_metadata` | io | No | ID |
| `write_audit` | io | No | ID |
| `publish_event` | io | No | ID |
| `notify_user` | io | No | ID |
| `delete_artifacts` | io | No | None |

### AWS Activities

| Activity | Service | Idempotency |
|---|---|---|
| `extract_to_s3` | S3 | Key from `run_id` |
| `process` | S3 | Derived key |
| `validate_output` | S3 | Read only |
| `load_table` | DynamoDB | `attribute_not_exists(pk)` |
| `publish_completion` | DynamoDB, SNS | Marker, then publish |
| `notify_user` | Secrets Manager, SQS | Not protected, so consumers must tolerate duplicates |
| `delete_outputs` | S3 | Deleting is naturally repeatable |

`classify` makes `AccessDenied`, `NoSuchBucket`, `NoSuchKey`, `ValidationException`, and `InvalidParameter` non-retryable. Anything else is retried.

## 7. Task Queues and Workers

| Queue | Handles | Started by |
|---|---|---|
| `hello` | Hello Workflows and Activity | `workers.hello_worker` |
| `checkout` | Checkout | `workers.observed_worker` |
| `orders` | `RoutedOrderWorkflow` | `workers.specialized --role orders` |
| `payments` | Charge, refund | `--role payments` |
| `inventory` | Reserve, release | `--role inventory` |
| `notifications` | Email | `--role notifications` |
| `data-processing` | `hash_block`, in a process pool | `--role data-processing` |
| `platform-orders` | Platform Workflows | `workers.platform --role orders` |
| `platform-payments` | Fraud, charge, refund | `--role payments` |
| `platform-fulfillment` | Reserve, release, ship | `--role fulfillment` |
| `platform-notifications` | Email | `--role notifications` |
| `capstone-orchestration` | Capstone Workflows | `capstone.worker --role orchestration` |
| `capstone-io` | IO Activities | `--role io` |
| `capstone-data` | Chunk, merge, and quality Activities | `--role data` |

Platform and capstone Activity Workers use 20 slots and a matching thread pool, and a 5 second graceful shutdown.

SDK 1.35 allows only one Worker per queue per Runtime, so run more copies as **separate processes**.

## 8. Key sequences

### Starting a Workflow and running an Activity

```text
Client          Service               Worker
  |  start          |                    |
  |---------------->| WORKFLOW_EXECUTION_STARTED
  |                 | WORKFLOW_TASK_SCHEDULED
  |                 |<------- poll ------|
  |                 | WORKFLOW_TASK_STARTED
  |                 |----- history ----->| runs code until it awaits
  |                 |<---- commands -----| ScheduleActivityTask
  |                 | WORKFLOW_TASK_COMPLETED
  |                 | ACTIVITY_TASK_SCHEDULED
  |                 |<------- poll ------|
  |                 |------- task ------>| runs the Activity
  |                 |<----- result ------|
  |                 | ACTIVITY_TASK_STARTED, ACTIVITY_TASK_COMPLETED
  |                 | WORKFLOW_TASK_SCHEDULED, and the code continues
  |<--- result -----| WORKFLOW_EXECUTION_COMPLETED
```

### Worker crash after a charge commits

```text
Worker 1         Bank        Service            Worker 2
  |  charge(key)  |              |                  |
  |-------------->| saved        |                  |
  X dies                         |                  |
                                 | Start-To-Close expires
                                 | schedules a retry
                                 |<----- poll ------|
                                 |------ task ----->|
                  |<----------- charge(same key) ---|
                  | recognised, returns the first charge
                                 |<----- result ----|
```

### Cancel during an Activity

```text
Client          Service           Workflow             Activity
  | cancel         |                  |                    |
  |--------------->|----------------->| await raises       |
  |                |                  | ActivityError      |
  |                |                  | (cancelled cause)  |
  |                |                  | is_cancelled = yes |
  |                |<-- run cleanup --|                    |
  |                |<-- cancelled ----| re-raise           |
```

Without `is_cancelled_exception`, an `except asyncio.CancelledError` skips this cleanup. That was a real bug here, and tests in `test_phase07_cancel_during_activity.py` now guard it.

### Continue-As-New in chunk processing

```text
Run 1: chunks 0, 1 --> continue_as_new(ChunkJob(next_index=2, finished=[a, b]))
Run 2: chunks 2, 3 --> continue_as_new(ChunkJob(next_index=4, finished=[a, b, c, d]))
Run 3: no chunks left --> returns all references to the parent
```

Same Workflow ID, a new Run ID each time, and a fresh history.

## 9. Capstone API

All endpoints need `Authorization: Bearer <token>`, or return `401`.

| Method | Path | Body | Success | Errors |
|---|---|---|---|---|
| `POST` | `/requests` | `request_id`, `source_name`, `notify_email`, optional `rows`, `null_fraction` | `202`, with `workflow_id` and `run_id` | |
| `GET` | `/requests/{id}` | | `200`, with a status | `404` |
| `POST` | `/requests/{id}/decision` | `approved` | `202` | |
| `PATCH` | `/requests/{id}/notify-email` | `email` | `200`, with `previous` | `409` if rejected |
| `DELETE` | `/requests/{id}` | | `202` | |

The Workflow ID is `data-request-{request_id}`. `WorkflowIDConflictPolicy.USE_EXISTING` makes `POST /requests` idempotent.

## 10. Supporting components

### Ledger schema

```sql
effects(id, kind, key, detail, idempotency_key UNIQUE)
stock(quantity)
```

`apply(kind, key, detail, idempotency_key)` returns the first result on a repeat. Tests call `count(kind, key)` to check what really happened.

### Encryption

| Setting | Value |
|---|---|
| Algorithm | AES-GCM |
| Nonce | 12 random bytes per payload, stored with the ciphertext |
| Payload encoding | `binary/encrypted` |
| Key selection | `key-id` metadata on each payload |
| Rotation | New data uses the active key, and old keys stay available for decoding |

### `diagnose()` verdicts

Checked in this order:

| Verdict | Evidence |
|---|---|
| `CLOSED` | Status isn't `RUNNING` |
| `NONDETERMINISM` | A `WORKFLOW_TASK_FAILED` message mentions nondeterminism |
| `WORKFLOW_TASK_FAILING` | Any other Workflow Task failure |
| `RETRY_STORM` | A pending Activity has reached 5 attempts |
| `NO_ACTIVITY_WORKER` | Pending Activity, attempt 1, state `SCHEDULED` |
| `ACTIVITY_RUNNING` | Pending Activities otherwise |
| `NO_WORKFLOW_WORKER` | Last event is `WORKFLOW_TASK_SCHEDULED` |
| `WAITING_FOR_TIMER_OR_MESSAGE` | None of the above |

`NO_ACTIVITY_WORKER` and `NO_WORKFLOW_WORKER` are strong hints, not proof. A healthy run can show either for a moment.

### Environment variables

| Variable | Used by | Purpose |
|---|---|---|
| `TEMPORAL_ADDRESS` | `shared/connection.py` | Server address |
| `TEMPORAL_NAMESPACE` | `shared/connection.py` | Namespace |
| `TEMPORAL_API_KEY` | `shared/connection.py` | API key |
| `TEMPORAL_CONFIG_FILE` | The SDK | TOML profile location |
| `CAPSTONE_API_TOKEN` | `capstone/api.py` | API bearer token |
| `CAPSTONE_KEY_BASE64` | `capstone/worker.py` | Enables payload encryption |
| `CODEC_TOKEN`, `CODEC_KEY_BASE64` | `scripts/codec_server.py` | Codec Server settings |

## 11. Known limitations

These are real gaps. Finding and fixing them makes good capstone extension work.

| Area | Limitation | Possible fix |
|---|---|---|
| Capstone cleanup | On failure, `delete_artifacts` removes files, but a metadata row or event already written stays, and can point at a deleted result | Add compensations for metadata and events |
| Capstone encryption | The API doesn't load the codec, so it can't read encrypted Workflows when `CAPSTONE_KEY_BASE64` is set | Connect the API with `data_converter()` |
| `source_ready` | Keeps a counter in memory, which only suits one Worker | Check the real source |
| `process_chunk` | Runs on threads, so real CPU work won't run in parallel | Use the process-pool setup |
| API | `decision` and `DELETE` on an unknown ID return `500` | Catch `RPCError`, and return `404` |
| Saga | Compensation is added after a step succeeds, so a lost reply on the final attempt leaves no undo | Register before the step, with a safe undo |
| `publish_completion` | A crash between marker and publish loses the event | Use an outbox |
| `notify_user` (AWS) | Has no duplicate protection | Consumers deduplicate on `run_id` |
| `Fault` access | `CapstoneActivities._run` calls a private method of `CommerceActivities` | Move faults to a shared helper |
| Search Attributes | `OrderState` must be registered, or runs stall | Register during deployment |

## 12. Test design

### Fixtures (`tests/conftest.py`)

| Fixture | Scope | Provides |
|---|---|---|
| `time_env` | Session | Time-skipping server |
| `local_env` | Session | Dev server, with `OrderState` registered |
| `ledger` | Test | A fresh SQLite ledger |
| `queue` | Test | A unique queue name |

All tests share one event loop, because a Temporal client belongs to the loop that created it.

### Choosing an environment

| Test needs | Use |
|---|---|
| Long timers | `time_env` |
| Search and visibility | `local_env` |
| Cancel while an Activity runs | `local_env`, because a running Activity pauses time skipping |
| An abandoned child | `local_env`. On `time_env` it stopped later tests skipping time |
| A real crash | `local_env`, with a subprocess Worker |
| One Activity alone | `ActivityEnvironment` |
| Compatibility with old code | `Replayer` |

### Rules that keep the suite reliable

- Each test uses a unique queue and order ID, because a shared server remembers everything.
- Assert on the Ledger, and not only on results.
- `pytest-timeout` fails any test after 120 seconds.

## 13. How to extend the code

### Add a Workflow

1. Create `src/my_work/my_flow.py`, with dataclass inputs.
2. Import Activities inside `imports_passed_through()`.
3. Set a Start-To-Close timeout on each Activity call.
4. Build idempotency keys from business IDs.
5. Add a Worker, or test it with one in a fixture.
6. Write tests for normal, failure, retry, cancellation, and replay.

### Add an Activity

1. Decide whether it's `def` or `async def`. Choose `def` if anything blocks.
2. Classify its errors as retryable or not.
3. Accept an idempotency key, or make it naturally repeatable.
4. Add a heartbeat if it can run long.
5. Register it on the Worker for its queue.

### Change a running Workflow

1. Check the table in [Phase 11](../journey/phase-11-versioning/lesson.md).
2. Use `workflow.patched`, or Worker Versioning.
3. Run a replay test against a saved history.
4. Plan how the patch will be removed.
