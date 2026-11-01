# Phase 6: Signals, Queries, and Updates

## 1. Objective

Build Workflows the outside world can talk to, and pick the right kind of message.

## 2. Mental model

Imagine an order being handled by someone in another building:

- **Signal:** you leave a note on their desk. You know it arrived, but not whether it's been read.
- **Query:** you look through the window at their whiteboard. Nothing changes.
- **Update:** you phone them, ask for a change, and they answer yes or no while you wait.

## 3. Temporal model

| | Signal | Query | Update |
|---|---|---|---|
| Changes state? | Yes | **Never** | Yes |
| Returns a value? | No | Yes | Yes |
| Written to history? | Yes | **No** | Yes, if accepted |
| Caller waits for | The Service accepting it | The Worker's answer | The Workflow finishing the handler |
| Needs a live Worker? | No, it's stored | **Yes** | Yes |
| Handler | `def` or `async def` | **`def` only** | `def` or `async def` |
| Can reject bad input? | No | No | **Yes, with a validator** |

### Which one?

| Situation | Use |
|---|---|
| "Payment arrived," fire and forget | Signal |
| A status page reads progress | Query |
| Change something, and the caller must know if it worked | Update |
| Do real work against the outside world | Activity, not a handler |
| Begin the process | Start the Workflow |

A rejected Update leaves no trace in history. In the test, a blank address raised `WorkflowUpdateFailedError`, and history contained no Update events at all.

Sources: [Message passing in Python](https://docs.temporal.io/develop/python/message-passing)

## 4. Python implementation

Read [workflows/interactive_order.py](../../temporal-learning/src/workflows/interactive_order.py). The states are:

```text
ORDER_CREATED -> WAITING_FOR_PAYMENT -> PAYMENT_CONFIRMED -> FULFILLING -> COMPLETED
```

It also handles `CANCELLED` and `PAYMENT_TIMED_OUT`.

```powershell
pytest tests/test_phase06_interactive.py -v
```

Four details are easy to miss:

**`@workflow.init`** runs before any handler, so a Signal that arrives before `run` starts still finds a complete object. Without it, attributes set inside `run` might not exist yet.

**Signals must be safe to repeat.** A duplicate `confirm_payment` changes nothing:

```python
if self.payment_reference is None:
    self.payment_reference = reference
```

**Signal-With-Start** solves a race: what if the signal is sent before the Workflow exists? It starts the Workflow and delivers the Signal in one step. See `test_signal_with_start_never_misses_the_first_signal`.

**`all_handlers_finished`** stops the run from closing while a handler is mid-flight.

**Testing note:** these Update tests happen to use a real dev server (`local_env`), but Updates work on the time-skipping server too. I checked, after wrongly assuming otherwise.

## 5. Exercise

In `src/my_work/discount_order.py`, extend the idea with an `apply_discount(percent)` Update. The validator should reject anything over 50%, and any discount after payment. The handler should return the new total. Add a `total` Query, and write tests.

## 6. Failure exercise

A developer writes this. Find the bugs.

```python
@workflow.query
async def status(self) -> str:
    self.query_count += 1
    return await workflow.execute_activity(load_status, ...)
```

Then answer: all your Workers are down, and support opens the status page. What does the Query do? What about a Signal sent at the same moment?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. Explain the difference between starting, signaling, querying, updating, and calling an Activity, with one real use for each.
2. Why must a Query be read-only? What would go wrong during replay otherwise?
3. Why should a Signal handler tolerate duplicates?
4. When is an Update better than a Signal followed by a Query?
5. What problem does `@workflow.init` solve?
6. Why did a rejected Update leave no history?

## 9. Gate

Type `/gate`.

## 10. Next step

**Phase 7: Timers and Long-Running Workflows.**
