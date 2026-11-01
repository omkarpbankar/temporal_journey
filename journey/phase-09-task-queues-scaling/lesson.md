# Phase 9: Task Queues, Workers, Scaling, and Routing

## 1. Objective

Design a Worker topology, and work out how much capacity it needs.

## 2. Mental model

A restaurant with one line for everything has a problem: one enormous catering order holds up every customer buying a coffee. So you open separate counters.

**Task Queues are those counters.** A slow or broken queue only affects its own line, and each line gets exactly the staff it needs.

## 3. Temporal model

Workers pull tasks only "when it has spare capacity," so they can't be overloaded the way a pushed system can. An overloaded system shows up as work *waiting* on a queue. That's why `schedule_to_start_latency` is the metric to watch.

One Workflow can send each Activity to a different queue:

```python
await workflow.execute_activity_method(..., task_queue="payments", ...)
```

### When should you split queues?

| Reason | Example |
|---|---|
| Failure isolation | A slow email provider mustn't delay payments |
| Different resources | CPU-heavy work needs other machines |
| Independent scaling | 100 notification slots, 10 payment slots |
| Rate limits | A partner API allows 5 calls per second |
| Security | Only some Workers hold the bank credentials |

Don't split without a reason. Each queue is one more thing to deploy and monitor. And **every Worker on a queue must handle everything sent to that queue.**

### Measuring capacity

Little's Law gives you a starting point:

```text
slots needed = arrival rate x time per task
```

For 10,000 Workflows an hour with 3 Activities of 2 seconds each:

```text
10,000 / 3,600 = 2.8 Workflows per second
2.8 x 3 = 8.3 Activities per second
8.3 x 2 seconds = about 17 slots in use at once
```

Add headroom for peaks and retries, then verify against real metrics. It's a first estimate, not a design.

## 4. Python implementation

Read [workflows/routed_order.py](../../temporal-learning/src/workflows/routed_order.py) and [workers/specialized.py](../../temporal-learning/src/workers/specialized.py).

```powershell
pytest tests/test_phase09_routing.py -v -s
```

### What I measured

| Experiment | Result |
|---|---|
| 6 tasks of 0.3s, 1 Worker, 1 slot | **2.03s** |
| 6 tasks of 0.3s, 3 Workers, 1 slot each | **0.69s** |
| Notification Workers all down | Customer charged, stock reserved, run waits. When Workers return, it finishes |

### Run it yourself

```powershell
python -m workers.specialized --role orders
python -m workers.specialized --role payments --concurrency 10
python -m workers.specialized --role inventory --concurrency 10
python -m workers.specialized --role notifications --concurrency 100
```

### Python details

**CPU work needs processes, not threads.** Python threads take turns because of the GIL, so CPU work in a `ThreadPoolExecutor` runs one at a time. The `data-processing` role uses a `ProcessPoolExecutor`, which needs a `SharedStateManager` so that heartbeats and cancellation can cross process boundaries.

**One Worker per queue per Runtime.** I tried starting three Workers on one queue in one process, and SDK 1.35 refused:

```text
Registration of multiple workers with overlapping worker task types on the
same namespace, task queue, and deployment build ID not allowed
```

That's a helpful rule, because real scaling means **separate processes**, which is what you'd run in production.

## 5. Exercise

Design a topology for the plan's scenario: 10,000 Workflows per hour, slow external APIs, CPU-heavy processing, and independent scaling. Write in `my-work/topology.md`:

- Your queues, and why each exists
- Worker types and `max_concurrent_activities` for each
- Your capacity calculation
- What you'd alert on

## 6. Failure exercise

Answer with reasons:

1. Payment Workers are overloaded. What do you see, and does anything fail?
2. Can notification processing affect payments in this design?
3. A new queue's Worker registers `charge_payment`, but the old queue's Worker doesn't. A running Workflow still schedules it on the old queue. What happens?
4. You set a Schedule-To-Start timeout of 1 second. A deploy restarts all Workers. What happens?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. How do Workers scale?
2. How should Task Queues be separated? Give a reason *not* to split.
3. How should CPU-heavy work be isolated in Python, and why?
4. How do you measure Worker capacity?
5. Why does an overload appear as latency, not as errors?

## 9. Gate

Type `/gate`.

## 10. Next step

**Phase 10: Testing.**
