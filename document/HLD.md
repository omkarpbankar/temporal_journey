# High-Level Design

This document describes the architecture of the code in this repository: how the pieces fit together, how work flows through them, and why they're built this way.

For exact contracts, such as timeouts, keys, and states, see the [Low-Level Design](LLD.md).

## 1. Purpose and scope

The repository has two jobs:

1. **Teach Temporal**, through lessons and small runnable labs.
2. **Show production thinking**, through two complete systems: the Order Fulfillment Platform (Phase 18) and the Data Processing Platform (the capstone).

**In scope:** Workflows, Activities, Workers, Clients, tests, observability, security, and deployment files.

**Out of scope:** Running a production cluster, real payment or shipping providers, and load testing. External systems are replaced with local stand-ins, as listed in section 10.

## 2. System context

```text
                          +----------------------------+
                          |         Learner            |
                          +-------------+--------------+
                                        |
                 reads lessons, runs code, asks questions
                                        |
       +--------------------------------+-------------------------------+
       |                                |                               |
       v                                v                               v
+--------------+              +------------------+            +------------------+
|  journey/    |              | temporal-learning|            | .claude/         |
|  lessons and |              | code and tests   |            | mentor rules and |
|  exercises   |              |                  |            | progress record  |
+--------------+              +--------+---------+            +------------------+
                                       |
                                       | gRPC, port 7233
                                       v
                              +------------------+
                              | Temporal Service |
                              | dev server, test |
                              | server, Cloud or |
                              | self-hosted      |
                              +------------------+
```

## 3. Architectural layers

```text
+---------------------------------------------------------------+
| Entry points      API (FastAPI), Clients, Schedules           |
+---------------------------------------------------------------+
| Orchestration     Workflows: deterministic, durable, no I/O   |
+---------------------------------------------------------------+
| Side effects      Activities: I/O, retried, idempotent        |
+---------------------------------------------------------------+
| Stand-in systems  Ledger, ObjectStore, moto                   |
+---------------------------------------------------------------+
| Shared support    Models, connection, encryption, diagnosis   |
+---------------------------------------------------------------+
```

The most important boundary is between **orchestration** and **side effects**:

| Workflow layer | Activity layer |
|---|---|
| Decides what happens next | Does one thing |
| Must be deterministic | May be nondeterministic |
| Reruns on replay | Skipped on replay, if already finished |
| No network, files, or clock | Network, files, and databases are all fine |
| State rebuilt from history | State kept in external systems |

## 4. Runtime architecture

The Service never runs your code. Workers pull tasks, run them, and report back.

```text
  YOUR INFRASTRUCTURE                          TEMPORAL SERVICE
 +-----------------------------+              +---------------------------+
 |                             |   start,     |                           |
 |  Client or API  ------------+--signal,-----+-> Frontend                |
 |                             |   query      |      |                    |
 |                             |              |      v                    |
 |  Worker                     |    poll      |   History (Event History, |
 |   +- poller  <--------------+--------------+-   timers, state)         |
 |   +- Workflow code          |   results    |      |                    |
 |   +- Activity code ---------+--------------+->    v                    |
 |        |                    |              |   Matching (Task Queues)  |
 |        v                    |              |      |                    |
 |  External systems           |              |      v                    |
 |  (bank, S3, database)       |              |   Persistence             |
 +-----------------------------+              +---------------------------+
```

Three consequences shape the design:

- **Workers only make outbound calls**, so they can run behind a firewall.
- **Work waits safely** on Task Queues when no Worker is available.
- **The Service detects failure through timeouts**, because it can't see inside a Worker.

## 5. The two reference systems

### 5.1 Order Fulfillment Platform

An order passes through fraud checks, optional human review, payment, inventory, and shipping. If a later step fails, earlier steps are undone with a Saga.

```text
                         platform-orders queue
                     +--------------------------+
 Client ------------>| OrderFulfillmentWorkflow |
                     |   PaymentChildWorkflow   |
                     +---+-------+---------+----+
                         |       |         |
        platform-payments|       |         |platform-notifications
                         v       |         v
              +-----------------+|  +--------------------+
              | Payment Workers ||  | Notification       |
              | fraud, charge,  ||  | Workers: email     |
              | refund          ||  +--------------------+
              +-----------------+|
                                 | platform-fulfillment
                                 v
                      +----------------------+
                      | Fulfillment Workers  |
                      | reserve, release,    |
                      | ship                 |
                      +----------------------+
```

The main flow:

```text
FRAUD_CHECK --(score below 0.7)--------------------------+
     |                                                   |
     +--(score 0.7 or more)--> REVIEW --(approved)-------+
                                 |                       v
                    (rejected or timed out)           PAYING
                                 |                       |
                                 v                       v
                             REJECTED                RESERVING
                                                         |
                                                         v
                         COMPENSATING <--(failure)-- SHIPPING
                               |                         |
                               v                         v
                       FAILED or CANCELLED           COMPLETED
```

### 5.2 Data Processing Platform (capstone)

A user submits a request through an API. The Workflow processes it through chunked work, waits for approval if quality is low, and then publishes the result.

```text
 User --HTTPS--> API --start, signal, query, update--> Temporal Service
                                                             |
             +-----------------------+-----------------------+
             |                       |                       |
   capstone-orchestration       capstone-io            capstone-data
             |                       |                       |
             v                       v                       v
   +--------------------+   +-----------------+    +-------------------+
   | Orchestration      |   | IO Workers      |    | Data Workers      |
   | Workers            |   | record, source, |    | process chunks,   |
   | DataRequestWorkflow|   | download, store,|    | merge, quality    |
   | ChunkProcessing    |   | metadata, audit,|    | check             |
   | Workflow           |   | event, notify   |    |                   |
   +--------------------+   +--------+--------+    +---------+---------+
                                     |                       |
                                     v                       v
                              +-----------------------------------+
                              |   ObjectStore (data) and Ledger   |
                              |   (metadata and side effects)     |
                              +-----------------------------------+
```

The flow of one request:

```text
Validate -> Create record -> Wait for source -> Download
    -> Process chunks (Child Workflow, Continue-As-New)
    -> Merge -> Quality check
         |
         +--(good)-----------------------------+
         |                                     |
         +--(low)--> Wait for human --approve--+
                          |                    v
                       reject             Store results
                          |                    |
                          v                    v
                   Clean up, notify     Update metadata
                                               |
                                               v
                                    Audit -> Publish -> Notify
```

## 6. Key design decisions

| Decision | Why | Trade-off |
|---|---|---|
| Keep Workflows free of I/O | They must replay deterministically | More Activities, and more code |
| Every side effect is idempotent | Activities can run more than once, and this is proven with a real crash test | Each external call needs a key |
| Derive keys from business IDs | A retry must look identical to the first attempt | IDs must be stable and unique |
| Pass references, not data | History is limited, and read on every replay | Needs an object store and retention planning |
| Separate Task Queues by workload | Failure isolation and independent scaling | More to deploy and monitor |
| Use a Saga, not transactions | You can't roll back a bank or a warehouse | Compensation must be designed, and can fail |
| Publish in two phases, staged first | Nobody sees unapproved data | One extra copy |
| Client-chosen request ID | Retrying the API can't start a second run | Clients must supply a good ID |
| Roll over with Continue-As-New | Keeps history bounded | State must be passed along by hand |
| Sync Activities on thread pools | Blocking libraries like boto3 and sqlite are safe | Thread pool sizing matters |
| Connection settings from the environment | The same code reaches local, self-hosted, or Cloud | Configuration must be managed |
| Encrypt in the client | The Service stores only ciphertext | Key management, and a Codec Server for the UI |

## 7. Reliability model

### Who handles each failure

| Failure | Detected by | Handled by | Outcome |
|---|---|---|---|
| Brief dependency error | Exception in the Activity | Retry policy | Retried with backoff |
| Permanent rejection | `ApplicationError(non_retryable=True)` | Workflow logic | Compensate, or fail |
| Worker crash | Start-To-Close timeout | Service retries the Activity | Resumes, and needs idempotency |
| Long Activity hang | Heartbeat timeout | Service retries it | Resumes from heartbeat details |
| Bug in Workflow code | Workflow Task failure | Task retries until you deploy a fix | Run stays open |
| Nondeterministic deploy | Replay mismatch | Deploy compatible code | Run stays open |
| No Workers at all | Tasks wait on the queue | Starts when Workers return | Nothing is lost |
| Dependency outage | Repeated failures | Retries, up to the policy limit | Recovers, or the Workflow decides |
| Human doesn't respond | `wait_condition` timeout | Workflow treats it as a rejection | Safe default |
| Cancellation | `is_cancelled_exception` | Workflow cleanup, then re-raise | Recorded as Cancelled |

### Execution guarantees

| Item | Guarantee |
|---|---|
| Workflow code | Effectively once, to completion |
| Activities | At least once, under the retry policy. **Not exactly once** |
| Signals | Stored durably, and handlers should tolerate duplicates |
| Updates | The caller gets a validated result |
| Queries | No state change, and they need a live Worker |

Exactly-once *effects* come from combining at-least-once execution with idempotent receivers. Temporal provides the first half, and this code provides the second.

## 8. Scaling

Each Worker role scales independently, by running more processes.

| Role | Work type | Scales with | Executor |
|---|---|---|---|
| Orchestration | Workflow Tasks, no I/O | Open and active Workflows | Event loop |
| IO | Waiting on networks | Slots | Thread pool |
| Data, in Phase 9 | CPU-bound | CPU cores | Process pool, because of the GIL |

Workers pull only when they have capacity, so overload shows up as **rising Schedule-To-Start latency**, not as errors. Capacity is estimated with Little's Law:

```text
slots needed = arrival rate x time per task
```

The capstone's `process_chunk` runs on a thread pool for simplicity. Real CPU-heavy chunks should use the process-pool setup from `workers/specialized.py`.

## 9. Cross-cutting concerns

### Security

```text
 YOUR NETWORK                                   | TEMPORAL SERVICE
                                                |
 API token --> API --+                          |
                     +-- TLS + API key/mTLS ----+--> Frontend
 Workers ------------+                          |      |
    |                                           |      v
    | plaintext and keys exist only here        |   Ciphertext at rest
    v                                           |
 Codec Server <-- authorised browser only       |
```

| Control | Implementation |
|---|---|
| Payload encryption | `EncryptionCodec`, AES-GCM with key IDs for rotation |
| Failure text encryption | `DefaultFailureConverterWithEncodedAttributes` |
| Secrets | Read in Activities, never in Workflows |
| API access | Bearer token, as a placeholder for real identity |
| Sensitive data | Keep out of Workflow IDs and Search Attributes, which aren't encrypted |

### Observability

| Signal | Source | Answers |
|---|---|---|
| Metrics | Prometheus endpoint on Workers | Is the system healthy? |
| History | Temporal UI, and `scripts/diagnose.py` | What happened to this run? |
| Logs | `workflow.logger`, `activity.logger` | What was the code doing? |
| Search Attributes | `OrderState` | Which runs are in this state? |
| Business metrics | `orders_completed`, `data_requests_completed` | Is the business working? |

### Versioning

| Strategy | Used for | Where |
|---|---|---|
| Worker Versioning, Pinned | Short and medium Workflows | Tested in Phase 11 |
| `workflow.patched` | Long-lived Workflows | Phase 11, and the capstone's audit step |
| Replay tests | Proof before deploying | `tests/histories/` |

### Testing strategy

```text
              +------------------+
              |   Crash tests    |  Real process, really killed
              +------------------+
              | Integration      |  Real dev server
              +------------------+
              | Time-skipping    |  Fast Workflow tests
              +------------------+
              | Replay           |  Old histories against new code
              +------------------+
              | Activity unit    |  ActivityEnvironment
              +------------------+
```

Tests check **real side effects** through the Ledger, because a double charge doesn't show in a Workflow's return value.

## 10. Deployment

### Local development

```text
+------------------+    +--------+    +--------+
| dev_server.py    |<---| Worker |    | Client |
| ports 7233, 8233 |<---+--------+    +---+----+
+------------------+<---------------------+
```

### Container view

`deploy/` builds one image, and the command chooses the role.

```text
+---------------+  +-----------+  +------------+  +---------+
| orchestration |  | io        |  | data x N   |  | api     |
+-------+-------+  +-----+-----+  +-----+------+  +----+----+
        |                |              |              |
        +-------+--------+--------------+--------------+
                |                       |
                v                       v
        Temporal Service         shared data volume
```

The compose file is for one host, and the shared volume stands in for object storage.

### Reference production view on AWS

This is a design, and hasn't been deployed.

```text
Clients --> API Gateway --> Lambda or ECS API ---------+
EventBridge rule --------------------------------------+
SQS consumer ------------------------------------------+
                                                       |
                                                       v
                                       Temporal Cloud or self-hosted
                                                       ^
                                       outbound polling only
                  +------------------+-----------------+
                  |                  |                 |
          ECS orchestration      ECS IO           ECS or EKS data
                  |                  |                 |
                  +---> CloudWatch   +---> S3, DynamoDB, RDS
                                     +---> SNS, SQS
                                     +---> Secrets Manager
```

### Stand-ins for production systems

| Stand-in | Represents | Replace with |
|---|---|---|
| `Ledger` (SQLite) | Bank, warehouse, email, metadata store | Real providers, and PostgreSQL |
| `ObjectStore` (folder) | S3 | S3 |
| moto | AWS, in tests only | Real AWS |
| Dev server | Temporal Service | Temporal Cloud, or a cluster |
| Bearer token | Identity | Your identity provider |
| `Fault` | Real outages | Removed. It exists only for labs |

## 11. Known limitations

| Limitation | Effect | Direction |
|---|---|---|
| Stand-in stores are local | Workers must share a filesystem | Use S3 and a database |
| Dev server only | Not tested on a multi-service cluster | Run the Phase 15 steps |
| Not load tested | Capacity numbers are estimates | Load test before production |
| Capstone cleanup is partial | See [LLD section 11](LLD.md#11-known-limitations) | Add compensation for published state |
| Saga registers undo after each step | A lost reply on the final attempt leaves no refund | Discussed in Phase 18 |
| One shared API token | No per-user access control | Real identity and authorization |

## 12. Reading this design with the course

| Section | Comes alive in |
|---|---|
| 3 Layers | Phases 3 and 4 |
| 4 Runtime | Phases 1 and 2 |
| 5 Systems | Phase 18 and the capstone |
| 6 Decisions | The capstone defence |
| 7 Reliability | Phases 4, 5, and 7 |
| 8 Scaling | Phase 9 |
| 9 Cross-cutting | Phases 10, 11, 13, and 14 |
| 10 Deployment | Phases 15 and 16 |

A good exercise: pick any decision in section 6, then argue the opposite to your mentor.
