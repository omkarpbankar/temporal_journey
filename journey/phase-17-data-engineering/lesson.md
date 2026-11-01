# Phase 17: Temporal for Data Engineering

## 1. Objective

Know when Temporal is the right tool for data work, and when it isn't.

## 2. Mental model

Ask what's at the centre of the problem:

- **A schedule and a graph of datasets:** "every night, rebuild these tables." That's a data orchestrator like Airflow or Dagster.
- **A process with a life of its own:** "this request needs a human, may wait three days, and must undo its work if rejected." That's Temporal.
- **Moving a message:** that's a queue.

## 3. Comparing the options

| Tool | Best at | Weak at |
|---|---|---|
| **Temporal** | Long-running stateful processes in code, one per entity, per-run human steps, failure recovery | Dataset lineage, a built-in backfill UI. You run Workers |
| **Airflow** | Scheduled DAGs and a huge connector library | High-frequency event-driven runs, per-entity flows, long human waits |
| **Dagster** | Asset-centred pipelines, lineage, data quality | General business processes |
| **AWS Step Functions** | AWS-native flows, no servers to run | Logic in JSON state language, AWS lock-in, service limits on duration and history |
| **EventBridge** | Routing events between services | It has no memory of a process |
| **SQS** | Decoupling, buffering one message | No orchestration or process state |
| **Lambda** | Short stateless functions | Long runs. It has a 15 minute limit |
| **Glue Workflows** | Glue job chains | Anything outside Glue |
| **Status table with pollers** | Very simple, short flows | Grows into a homemade engine |

Service limits change, so check current AWS docs before relying on one.

**Temporal doesn't replace Airflow.** Plenty of good systems use both: Airflow for the nightly schedule, and Temporal for the complicated per-tenant runs it triggers.

### Questions to ask

1. Is the unit of work a dataset or an entity? 
2. Does a run need to wait for an event or a person?
3. How much do you need lineage and backfills?
4. Does a failure need business logic to undo it?
5. Who operates it, and what do they already know?

## 4. Python implementation

Read [workflows/data_pipeline.py](../../temporal-learning/src/workflows/data_pipeline.py) and [activities/data_pipeline.py](../../temporal-learning/src/activities/data_pipeline.py).

```powershell
pytest tests/test_phase17_data_pipeline.py -v
```

```text
DataPipelineWorkflow
   +-- extract          per partition, in parallel
   +-- validate
   +-- transform
   +-- load_staging
   +-- quality_check    below threshold: wait for a human
   +-- publish
```

| Plan requirement | How |
|---|---|
| Retries | `STEP` policy, with `BadSchema` non-retryable |
| Dependency handling | Each step's output feeds the next |
| Partial failure recovery | Failed partitions wait. Only they rerun |
| Manual approval Signal | `decide(approved)` |
| Status Query | `status()` |
| Cancellation | Tested at two different points |
| Compensation | `delete_staging` on rejection or cancel |

### Partial failure is the highlight

If `p2` fails, `p1` and `p3` keep their results. The Workflow waits, possibly for days, for someone to fix the bug and send `retry_failed_partitions`, or to give up with `drop_failed_partitions`. Only `p2` reruns.

Doing this in a scheduler usually means clearing task state by hand. Here it's a loop in Python.

### Two-phase publishing

Writing to `staging` and then publishing means consumers never see a half-finished or unapproved dataset. Compensation is simple, because it only deletes staging.

### Keep the data out of the Workflow

The Workflow only sees `DatasetRef`s, so a 10 GB partition costs the same history as a 10 row one.

**Python detail:** `gather(..., return_exceptions=True)` returns exceptions as values. So a cancel can come back as a value too, which is why `_process` checks `is_cancelled_exception` on each outcome. A test caught that.

## 5. Exercise

Write a decision record in `my-work/orchestrator-choice.md` for your own or an imagined data platform. Choose among Temporal, Airflow, and Step Functions for: a nightly warehouse load, a per-customer data export with approval, and a three-step file conversion triggered by S3 uploads.

## 6. Failure exercise

`publish` succeeds, and then the Worker dies before reporting. On retry it republishes the same file. Is that a problem here? Now change the design so that publish *appends* to a shared table. What must change?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. Temporal vs Airflow: when would you choose each?
2. Temporal vs Step Functions?
3. Temporal vs SQS?
4. Temporal vs EventBridge?
5. Temporal vs a database-backed state machine?
6. Why not use Temporal for everything?

## 9. Gate

Type `/gate`. You'll choose a tool for a scenario, and defend the choice.

## 10. Next step

**Phase 18: Advanced Patterns.**
