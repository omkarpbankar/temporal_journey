# Phase 4: Activities

## 1. Objective

Design an Activity with the right timeout, retry policy, idempotency, heartbeat, and cancellation behaviour.

## 2. Mental model

A Workflow is the manager with a plan. An Activity is the courier sent out into the real world, where roads close and parcels go missing.

Think like someone hiring a courier:

- **Timeout:** when do you assume the courier is lost?
- **Retry policy:** how many times do you send someone else?
- **Idempotency:** if two couriers deliver the same parcel, does the customer get charged twice?
- **Heartbeat:** a courier on a 10-hour trip who phones in every hour.

## 3. Temporal model

### Timeouts

```text
Scheduled ------------ Started ------------ Closed
|-- Schedule-To-Start --|-- Start-To-Close --|
|------------- Schedule-To-Close ------------|
```

| Timeout | Limits | Default | Guidance from the docs |
|---|---|---|---|
| Start-To-Close | One attempt | None | "We strongly recommend setting" it. It's how crashes are detected |
| Schedule-To-Close | The whole Activity, all retries | Infinity | Limits total time |
| Schedule-To-Start | Time waiting on the queue | Infinity | Usually monitor the metric instead |
| Heartbeat | Time between heartbeats | Off | Use a short one for long-running Activities |

An Activity must set Start-To-Close or Schedule-To-Close, or it's rejected.

### Retries

| `RetryPolicy` field | Default |
|---|---|
| `initial_interval` | 1 second |
| `backoff_coefficient` | 2.0 |
| `maximum_interval` | 100 times the initial interval |
| `maximum_attempts` | Unlimited |
| `non_retryable_error_types` | Empty |

"Unlimited" is the default. An Activity with a permanent problem retries forever unless you classify the error. Ask of every failure: can waiting fix this? A 503 can. A declined card can't.

Failed attempts that get retried **write no event to history**, which keeps history small. Tested in `test_fail_once_then_succeed`. To see them, look at the Activity's pending details.

Sources: [Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures), [Failure detection in Python](https://docs.temporal.io/develop/python/failure-detection)

## 4. Python implementation

Read [activities/commerce.py](../../temporal-learning/src/activities/commerce.py) and [workflows/checkout.py](../../temporal-learning/src/workflows/checkout.py). Then compare `checkout.py` with your Phase 1 `checkout_v2.py`. All the bookkeeping is gone.

### Sync vs async Activities

| Style | Runs on | Blocking calls? |
|---|---|---|
| `def` | A thread pool you give the Worker (`activity_executor`) | Fine |
| `async def` | The Worker's event loop | **Never.** One blocking call freezes all of them |

Rule of thumb: use `async def` only if every library inside it is truly async.

### Idempotency

The key must be **stable across retries**, so derive it from the business, not from the clock:

```python
ChargeRequest(order.order_id, order.amount_cents, f"{order.order_id}-charge")
```

A key built with `uuid4()` *inside the Activity* would differ on each attempt, and protect nothing.

### Heartbeats

[activities/long_running.py](../../temporal-learning/src/activities/long_running.py) reports progress and resumes from it. In the real run, the Activity failed at row 15 of 20, and the retry produced:

```text
ExportResult(rows_this_attempt=5, attempt=2, resumed_from=15)
```

**A retry starts your function from the top.** Heartbeat details are how it knows where to resume. Heartbeats are also how an Activity receives cancellation: "If the Heartbeat is not invoked, the Activity cannot receive a cancellation request."

### Run the labs

```powershell
pytest tests/test_phase04_activities.py -v
pytest tests/test_phase04_worker_crash.py -v
```

These cover fail once, fail three times, fail permanently, sleep past the timeout, crash, and duplicate execution. The crash test kills a real process right after the charge commits:

| Idempotency key? | Charges after recovery |
|---|---|
| Yes | **1** |
| No | **2** |

Also notice `test_out_of_stock_leaves_the_customer_charged`. Temporal did everything right, and the customer still paid for nothing. That needs compensation (Phase 8 and Phase 18), which only you can design.

## 5. Exercise

In `src/my_work/ship.py`, write `book_courier(order_id)` that calls a pretend courier API. Choose its timeouts, retry policy, and idempotency key, and classify three kinds of failure as retryable or not. Write down *why* for each choice.

## 6. Failure exercise

```python
@activity.defn
async def charge(order_id: str, amount: int) -> str:
    key = str(uuid.uuid4())
    response = requests.post(BANK_URL, json={"amount": amount, "key": key})
    return response.json()["id"]
```

Find at least three separate problems. For each, describe what goes wrong in production.

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. Why must an Activity be designed as if it can run more than once? Give the exact moment it happens.
2. Which timeout detects a Worker crash, and why?
3. When should an Activity *not* retry?
4. What happens if a long Activity has no heartbeat timeout and its Worker hangs?
5. Why can't an idempotency key be generated inside the Activity?
6. How should you choose a Start-To-Close value?

## 9. Gate

Type `/gate`. You'll design one Activity fully, with reasons.

## 10. Next step

**Phase 5: Failure Semantics.**
