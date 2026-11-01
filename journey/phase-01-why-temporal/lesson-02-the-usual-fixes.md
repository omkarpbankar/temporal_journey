# Lesson 2: The Usual Fixes

## 1. Objective

See exactly how far each common approach gets you, so you know which problem Temporal solves and which it doesn't.

## 2. Mental model

Your `checkout_v2.py` was the start of a job engine. You saved progress, skipped finished work, and then found a gap. Keep going down that road and you end up building this list:

| You need | You build |
|---|---|
| Something to restart crashed jobs | A cron job or supervisor |
| Progress that survives a crash | A status table |
| Retries that last hours | A `next_retry_at` column and a poller |
| Waiting 3 days for a human | A `waiting_since` column and another poller |
| Two pollers not grabbing the same job | Row locks |
| "Where is order 123 stuck?" | An admin page |

Every line above is ordinary and sensible. Together they are a homemade workflow engine, and your business logic is now scattered across all of them.

## 3. How each approach falls short

### Retries in application code

```python
@retry(attempts=3)
def charge(): ...
```

This handles a brief network blip. It doesn't survive the process dying, because the retry counter lives in RAM. It also can't tell a failed charge from one whose reply was lost.

### Cron jobs

Cron answers "when should this start?" It says nothing about what happened last time. A run that crashes halfway leaves no trace of where it stopped, and an overrunning job can overlap the next one.

### Queues (SQS, RabbitMQ, Kafka)

A queue makes one message survive a crash, which is a real improvement. But:

- Delivery is usually **at-least-once**, so the duplicate problem is still yours.
- A process with four steps becomes four consumers and four message types. The flow exists only in your head, and no single place shows it.
- Waiting days for something needs a delay trick or a database.

### Database-backed state machine

A `status` column is a serious design, and many good systems use one. It's the closest thing to Temporal, and also where the most code goes:

```text
PENDING -> CHARGED -> RESERVED -> EMAILED
```

You now own the pollers, locks, retry timing, timeouts, and stuck-row alerts. Look again at the crash from `checkout_v2`: the moment between "bank charged" and "row updated" still exists.

### Airflow-style orchestrators

Airflow is excellent for its job: scheduled batches of tasks in a known graph, usually data pipelines. It's built around scheduled DAG runs. It's not designed for one run per customer, thousands per minute, each waiting days for its own event.

Temporal doesn't replace Airflow. Phase 17 compares them properly.

## 4. Python implementation

No new code in this lesson. Look at your `checkout_v2.py` again and count the lines of *bookkeeping* against the lines of *business logic*. The comparison will matter when you see Temporal's version in Phase 4.

## 5. Exercise

Add a row to this table in `my-work/usual-fixes.md`, for each of these: retries, cron, queue, status table, Airflow.

| Approach | Survives a process crash? | Retries for hours? | Can wait 3 days for a person? | Stops double charges? | Shows where a run is stuck? |
|---|---|---|---|---|---|

## 6. Failure exercise

You use SQS with a 30 second visibility timeout. Your consumer takes 45 seconds to charge a card.

What happens? Would making the timeout 10 minutes fix it, or only hide it?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. Why isn't "at-least-once delivery" the same as "the job ran once"?
2. Give a real case where a status table is a *better* choice than Temporal. (Hint: think about how much of a process there is.)
3. Why does splitting a process across queue consumers make it harder to change?
4. Cron runs a job at 2:00. It crashes at 2:10. What exactly does cron know?

## 9. Gate

Part of the Phase 1 gate.

## 10. Next step

**Lesson 3: Durable Execution.** What if the notebook were kept automatically, for every line?
