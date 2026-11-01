# Phase 7: Timers, Cancellation, and Long-Running Workflows

## 1. Objective

Build Workflows that live for months, keep their history under control, and stop cleanly.

## 2. Mental model

A subscription that lives for 10 years is a process that's asleep almost all the time. While it sleeps, nothing is running: it costs a row in a database and a timer on the Service.

The catch is that the notebook keeps growing. Every step adds lines, and replay reads all of them. So every so often, you start a fresh notebook and copy over only what matters: "customer 12, 36 payments made." That's **Continue-As-New**.

## 3. Temporal model

### Durable timers

`await asyncio.sleep(...)` becomes `TIMER_STARTED` in history, and the Service owns the clock. In my lab, 360 simulated days ran in about **1.3 seconds** on the time-skipping server.

### History has limits

The limit is 51,200 Events or 50 MB, with a warning at 10,240 Events or 10 MB. This applies to all deployments. Your Workflow can't outrun it by being clever, so a long-lived loop has to roll over.

```python
if workflow.info().is_continue_as_new_suggested():
    await workflow.wait_condition(workflow.all_handlers_finished)
    workflow.continue_as_new(new_input)
```

Continue-As-New closes this run and starts a new one with the same Workflow ID, a new Run ID, and fresh history. State only survives if **you pass it as input**. In the lab, each run held 38 events, however long the subscription lived.

Don't call it from a handler. Wait for handlers in the main Workflow first, as the docs advise.

Sources: [Continue-As-New in Python](https://docs.temporal.io/develop/python/continue-as-new), [Limits](https://docs.temporal.io/cloud/limits)

## 4. Python implementation

Read [workflows/subscription.py](../../temporal-learning/src/workflows/subscription.py).

```powershell
pytest tests/test_phase07_subscription.py tests/test_phase07_cancel_during_activity.py -v -s
```

### Waiting for "this OR that"

```python
try:
    await workflow.wait_condition(lambda: self.cancel_requested, timeout=timedelta(days=30))
    # Condition became true: the customer cancelled.
except asyncio.TimeoutError:
    # Thirty days passed: time to renew.
```

### Cancellation: the bug I shipped, and what it taught me

I wrote cleanup like this, and every test passed:

```python
except asyncio.CancelledError:
    await cleanup()
    raise
```

Then I thought harder and wrote a test that cancels **while an Activity is running**. It failed. A customer cancelled during shipping wasn't refunded.

Here's why. A cancel arrives in different forms, depending on where the Workflow is waiting:

| Waiting on | The cancel arrives as |
|---|---|
| A timer or `wait_condition` | `asyncio.CancelledError` |
| An Activity | `ActivityError`, with a cancelled cause |
| A Child Workflow | `ChildWorkflowError`, with a cancelled cause |

So the correct pattern is:

```python
except (asyncio.CancelledError, ActivityError, ChildWorkflowError) as error:
    if not is_cancelled_exception(error):
        raise
    await asyncio.shield(cleanup())
    raise
```

**The lesson goes beyond cancellation:** my first tests only cancelled at the convenient moment. Always ask, "what if it happens in the other place?"

The final `raise` matters too: without it, the Workflow would finish as Completed, not Cancelled.

### Activities only hear a cancel through heartbeats

An Activity that doesn't heartbeat can't receive a cancel request.

### A trap in time-skipping tests

Two facts cost me a failed test:

- The server only skips time automatically while something awaits a result.
- A running Activity pauses skipping.

So a test that cancels mid-Activity uses a real server with a very short "month" (`renewal_days=0.00001`).

## 5. Exercise

In `src/my_work/trial.py`, build a 14-day free trial. Remind the customer at day 12, convert at day 14 unless they cancel, and allow a `extend_trial(days)` Update. Test it with time skipping, including a cancel at day 13.

## 6. Failure exercise

```python
class Subscription:
    def __init__(self): self.invoices = []

    @workflow.run
    async def run(self, input):
        while True:
            await asyncio.sleep(86400 * 30)
            self.invoices.append(await charge(...))
            if workflow.info().is_continue_as_new_suggested():
                workflow.continue_as_new(input)
```

What's lost at the rollover? Is the charge safe to retry? What else might be lost if a Signal handler is running at that moment?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. When should a Workflow use Continue-As-New?
2. What survives Continue-As-New, and what doesn't?
3. Why doesn't a sleeping Workflow need a Worker?
4. How does a cancel reach a Workflow waiting on an Activity?
5. How does a cancel reach the Activity itself?
6. Why must you re-raise after cleanup?

## 9. Gate

Type `/gate`.

## 10. Next step

**Phase 8: Child Workflows and Composition.**
