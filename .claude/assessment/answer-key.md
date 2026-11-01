# Final Assessment Answer Key

For the mentor. Don't show this to the learner before they've attempted the assessment.

## Part B: broken_apps.py

| App | Bug | Fix |
|---|---|---|
| 1 | A cancel during `ship` arrives as `ActivityError`, not `CancelledError`, so the refund is skipped | Catch both, and test with `is_cancelled_exception` |
| 2 | `OrderStage` isn't registered, so the Workflow Task fails. Runs stay `RUNNING`, with no failed Workflows | Register the attribute before deploying |
| 3 | `deprecate_patch` moved above `step_a`. It must stay exactly where `patched` was | Move it between `step_a` and `step_c` |
| 4 | The ID uses `customer_id`, so a customer's second order collides with an open abandoned child | Use the order ID, or something unique to the run |
| 5 | `except ActivityError` treats a cancellation as a declined charge | Re-raise when `is_cancelled_exception(error)` is true |

## Part D notes

1. The Activity retries after Start-To-Close, so it must be idempotent.
2. Retries continue under the policy. Check Schedule-To-Close and `maximum_attempts`. Unbounded retry with a capped interval keeps going.
3. Open runs replay and fail Workflow Tasks with nondeterminism. They stall, rather than fail. Roll back, or use Pinned versioning or patching.
4. It's recorded in history and processed on the next Workflow Task. The handler runs; the Activity continues.
5. Depends on the design. Compensation should be shielded, and must tolerate a second cancel.
6. Outcome unknown. Reconcile with the provider, or register a compensation safe when nothing happened.
7. Workers can't report results. Activities finishing meanwhile retry reporting or time out. Durable state is safe. Clients can't start runs.
8. Oversized payload: Workflow Task or Activity completion fails. Needs a redesign to references.
9. Open and closed runs with that key can't be decoded. Prevent with key management and backups.
10. Same Workflow ID. One wins, and the policy decides what the other gets. `USE_EXISTING` returns the running one.

## Part F: review_this.py

1. Nondeterminism: `datetime.now()` in a Workflow.
2. Nondeterminism: `random.random()` in a Workflow.
3. Inappropriate Workflow logic: `requests.get` in a Workflow. Belongs in an Activity.
4. Mutating `order.amount` from an unrecorded external value makes replay diverge.
5. Sensitive data: `card_number` in a payload, stored in history. Use a provider token.
6. Blocking call in `async def`: `requests.post` freezes the event loop. Use a sync Activity or an async client.
7. Non-idempotent Activity: `charge` has no key, so a retry double-charges.
8. Missing timeout: `charge` has neither Start-To-Close nor Schedule-To-Close, so it's rejected.
9. Incorrect retry policy: `maximum_attempts=0` means unlimited, with no non-retryable types. A declined card retries forever. HTTP errors also aren't checked with `raise_for_status`.
10. Query mutates state: `get_status` changes `self.status`.
11. Unsafe version change: `fraud_report` was inserted before an existing step with no patch or Worker Versioning.
12. Inappropriate timeout: a 30 day Start-To-Close with no heartbeat means a crash goes undetected for 30 days. Probably a Child Workflow or a timer.
13. Poor task queue design: one company-wide queue gives no isolation, or independent scaling.
14. `send_receipt` has no idempotency key either, and doesn't check the response.
