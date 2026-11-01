# Phase 13: Production Observability

## 1. Objective

Investigate a failed or stuck run using only metrics and Workflow history.

## 2. Mental model

Three tools answer three different questions:

| Tool | Question | Analogy |
|---|---|---|
| **Metrics** | Is the system healthy overall? | The car's dashboard |
| **Workflow history** | What happened to this one run? | The flight recorder |
| **Logs** | What was my code thinking? | The driver's notes |

Metrics tell you *that* something's wrong. History tells you *what*. Logs tell you *why*.

## 3. Temporal model

### Key SDK metrics

All have the `temporal_` prefix. I confirmed these appear on a running Worker's `/metrics` endpoint, except where marked.

| You want | Metric | Type |
|---|---|---|
| Workflow success | `workflow_completed` | Counter |
| Workflow failure | `workflow_failed` (from docs) | Counter |
| Cancellations | `workflow_canceled` (from docs) | Counter |
| Activity failures | `activity_execution_failed` | Counter |
| Activity latency | `activity_execution_latency` | Histogram |
| Workflow duration | `workflow_endtoend_latency` | Histogram |
| **Backlog** | `activity_schedule_to_start_latency`, `workflow_task_schedule_to_start_latency` | Histogram |
| Worker availability | `num_pollers`, `worker_task_slots_available` | Gauge |

**There's no direct metric for retries or for Activity timeouts** on the SDK metrics page. Each failed attempt does count in `activity_execution_failed`, so a rising rate is your retry signal. The attempt number on a pending Activity shows it for one run. Server and Cloud metrics give more, which is worth checking for your deployment.

**Backlog shows as latency.** Workers pull work, so an overloaded system doesn't throw errors. Tasks just wait longer. Schedule-To-Start is your most important capacity signal.

Source: [SDK metrics](https://docs.temporal.io/references/sdk-metrics)

### Reading a stuck Workflow

| Symptom | Likely cause |
|---|---|
| Last event is `WORKFLOW_TASK_SCHEDULED`, and it isn't moving | No Workflow Worker |
| Pending Activity, attempt 1, state `SCHEDULED` | No Activity Worker |
| Pending Activity with a high attempt count | Retry storm: read `last_failure` |
| `WORKFLOW_TASK_FAILED` events repeating | A bug, or nondeterminism |
| Last event is `TIMER_STARTED`, with nothing pending | Waiting normally |

## 4. Python implementation

```powershell
pytest tests/test_phase13_observability.py -v -s
```

Read [shared/diagnose.py](../../temporal-learning/src/shared/diagnose.py). It applies the table above to a live run. Each verdict is tested against a simulated incident:

```powershell
python scripts/diagnose.py <workflow id>
```

```text
Status:  RUNNING
Verdict: NONDETERMINISM
  - history_length=17
  - workflow_task_failed: [TMPRL1100] Nondeterminism error: ...
```

### Metrics

[workers/observed_worker.py](../../temporal-learning/src/workers/observed_worker.py) exposes Prometheus metrics on port 9000:

```python
Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address="0.0.0.0:9000")))
```

### Logging

`workflow.logger` and `activity.logger` add context like the Workflow ID. `workflow.logger` also stays quiet during replay, so you don't get duplicate lines. A plain `print` in a Workflow will repeat on every replay.

### Custom metrics

```python
workflow.metric_meter().create_counter("orders_completed").add(1)
```

### Dashboard and alerts

[observability/](../../temporal-learning/observability/) has a Prometheus config, alert rules, and a Grafana dashboard covering all ten metrics your plan lists. **I validated their structure, but I haven't loaded them into a real Prometheus or Grafana.** The Worker metrics they query are real, but expect to adjust the panels.

### Incident runbook

| Incident | Find it by | First action |
|---|---|---|
| Worker outage | `num_pollers` is 0, Schedule-To-Start climbs | Restart Workers. No data is lost |
| Dependency outage | `activity_execution_failed` rate climbs | Fix or wait. Retries resume by themselves |
| Backlog | Schedule-To-Start climbs while pollers are up | Scale that queue's Workers |
| Bad deploy | Many `WORKFLOW_TASK_FAILED` | Roll back or fix. Don't terminate |
| Runaway retries | High attempts, one error repeating | Fix the cause, or classify it non-retryable |
| Stuck Workflow | `diagnose.py` | Depends on the verdict |
| Nondeterminism | `TMPRL1100` in task failures | Deploy compatible code, using a replay test |

**Don't terminate runs during a bad deploy.** They're safely paused, and terminating throws away what a fix could rescue.

## 5. Exercise

Run the observed Worker and the Prometheus setup if you have Docker, or just read `/metrics` in your browser. Cause a retry storm with a bad Activity, and find it using only `/metrics` and `diagnose.py`.

## 6. Failure exercise

At 09:00 `workflow_completed` drops to zero. `num_pollers` looks normal, there are no Activity failures, and `workflow_task_schedule_to_start_latency` is low. Nothing is erroring in Activities. What are your top two hypotheses, and what do you check next?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. How do you investigate a stuck Workflow, step by step?
2. How do you identify a retry storm?
3. What would you alert on, and why not on every Activity failure?
4. Why does an overload appear as latency?
5. Why use `workflow.logger` rather than `print`?

## 9. Gate

Type `/gate`. You'll get metrics and a history, and must produce a root-cause hypothesis.

## 10. Next step

**Phase 14: Security.**
