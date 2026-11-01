# Phase 18: Advanced Patterns

## 1. Objective

Move from writing Temporal code to designing systems with it.

## 2. Mental model

An architect's job is to decide what happens **when things go wrong in the middle**. In a monolith, a database transaction rolls everything back. Across a bank, a warehouse, and a courier, nothing can roll back. You can only do new work that undoes old work, and that's a **Saga**.

You can't un-send an email or un-charge a card. You can send an apology, or issue a refund.

## 3. Temporal model

### Saga

```python
saga = Saga()
try:
    await charge()
    saga.add(refund)
    await reserve()
    saga.add(release)
    await ship()
except ...:
    await saga.compensate()   # newest first
```

The list of undo steps lives in plain Workflow memory, and replay rebuilds it after a crash. Without Temporal, that list needs a table, and a crash in the middle of undoing needs its own recovery.

**A real design decision.** The code adds `refund` *after* the charge succeeds. Suppose the charge's reply was lost on the *final* attempt: the bank charged, but the Workflow saw a failure, so no refund runs. Many designs register the compensation **before** the step, which requires an undo that's safe when nothing happened. Which is right depends on your provider. Think about this one, because the failure exercise below returns to it.

**Compensation can fail too.** `Saga.compensate` carries on past a failed undo and reports it, so a human can step in. Tested in `test_failed_compensation_is_reported_for_a_human`.

### Other patterns

| Pattern | How | Where |
|---|---|---|
| Fan-out/fan-in | `asyncio.gather`, with a `Semaphore` limit | `FanOutWorkflow` |
| Rate limiting | `max_task_queue_activities_per_second` on the Worker | Test below |
| Batching | Chunks plus Continue-As-New | `BatchWorkflow` |
| Scheduled and recurring | `client.create_schedule`, rather than cron on a Workflow | `test_schedule_runs_a_workflow` |
| External coordination | `get_external_workflow_handle(id).signal(...)` | `NotifierWorkflow` |
| Human in the loop | `wait_condition` with a timeout, then escalate | `HumanApprovalWorkflow` |
| Search and visibility | Search Attributes | `ORDER_STATE` |
| Backpressure | Workers pull, and slots limit intake | Phase 9 |
| Graceful shutdown | `graceful_shutdown_timeout` | `test_graceful_shutdown_lets_an_activity_finish` |
| Multi-tenant | See below | Design exercise |

Measured: 10 Activities at a limit of 5 per second took **4.94 seconds**.

### Multi-tenant designs

| Approach | Isolation | Cost |
|---|---|---|
| One namespace, tenant in Workflow ID and Search Attribute | Low | Simple |
| Task Queue per large tenant | Medium, noisy neighbours are contained | More Workers |
| Namespace per tenant | High | Most to operate |

A common shape: most tenants share, and the big ones get their own queues. The task queue `priority` and fairness options in the SDK are worth reading about as well.

## 4. Python implementation

### The Order Fulfillment Platform

Read [workflows/fulfillment_platform.py](../../temporal-learning/src/workflows/fulfillment_platform.py), starting at `run()`, then [workers/platform.py](../../temporal-learning/src/workers/platform.py).

```powershell
pytest tests/test_phase18_platform.py -v -s
```

| Requirement | Where |
|---|---|
| Order, payment, fraud, inventory, shipping, notification | Activities and a child |
| Cancellation | `request_cancel`, and cancelling the run itself |
| Compensation | `Saga` |
| Human approval | `_review`, for fraud scores of 0.7 or more |
| Retries, timeouts | `RETRY`, `STEP` |
| Signals, Queries, Updates | `submit_review`, `status`, `change_address` |
| Child Workflows | `PaymentChildWorkflow` |
| Task queues, Worker types | Four roles |
| Observability | `ORDER_STATE` and an `orders_completed` counter |
| Tests | 17 tests, plus 1 for cancelling mid-shipping |
| Versioned deployment | See Phase 11. Use Worker Versioning or `patched` |

### A trap: Search Attributes must be registered

My first cancel test hung. The Workflow Task kept failing, because `OrderState` wasn't registered on that server. Upserting an unknown attribute doesn't raise an error you'd catch in a normal way: the run just stalls. So:

- **Register attributes before deploying**, as a step in your deployment.
- On the CLI: `temporal operator search-attribute create --name OrderState --type Keyword`.
- Don't put sensitive values in them, because they aren't encrypted.

### Another trap: type hints are read early

`def ship(self, request: "ShipRequest")` failed at import with `NameError`, because `@activity.defn` evaluates hints when it runs. Define dataclasses before the Activities that use them.

## 5. Exercise

Add a `gift_wrap` step between reserving and shipping, with its own compensation. Update the Saga, and write tests for a failure at each point. Then choose: register the compensation before or after the step? Justify it.

## 6. Failure exercise

`charge_payment` fails on its final attempt with a timeout, so the Workflow doesn't know if the money moved. `saga.compensate()` finds no refund registered.

What state might the customer be in? Propose two fixes, and say what each needs from the payment provider.

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. Why is a Saga needed, and how does it differ from a database transaction?
2. Why does compensation run newest-first?
3. What should happen when a compensation fails?
4. How would you stop one large tenant from hurting others?
5. When would you use a Schedule rather than a sleeping loop?
6. Rate limit vs concurrency limit: what's the difference?

## 9. Gate

Type `/gate`. This one's the first real design test.

## 10. Next step

**Phase 19: SDK Internals.**
