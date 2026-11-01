# Phase 5: Temporal Failure Semantics

## 1. Objective

Given any incident, say what failed, where, whether Temporal retries it, and how to recover.

## 2. Mental model

Failures come in three layers, and each has a different owner:

| Layer | Example | Who deals with it |
|---|---|---|
| **The attempt** | One API call returned 503 | Temporal retries it, using your policy |
| **The Activity** | All retries are used up | Your Workflow decides |
| **The Workflow** | The business process can't go on | Your Workflow ends, or a human steps in |

Most confusion comes from not knowing which layer you're talking about.

## 3. Temporal model

### The surprise: a Python bug doesn't fail your Workflow

| Raised in Workflow code | Result |
|---|---|
| `ApplicationError` | **The Workflow fails**, permanently. It's a deliberate decision |
| `KeyError`, `TypeError`, and most other exceptions | **The Workflow Task fails** and retries. The run stays `RUNNING` |

Why? Temporal assumes a plain exception is a bug in your code. If it failed the run, you'd lose it for good. By retrying the task, it gives you time to deploy a fix, and the run continues where it stopped. Tested in `test_python_bug_keeps_the_workflow_open`.

The danger is the reverse: a bug can look like a run that's quietly stuck, not one that failed. Phase 13 teaches you to spot it.

### The error shapes

An Activity failure reaches your Workflow wrapped in layers:

```text
ActivityError                retry_state tells you why it stopped
  cause: ApplicationError    the actual reason (or TimeoutError)
```

| What happened | `.cause` | `retry_state` |
|---|---|---|
| Ran out of attempts | `ApplicationError` | `MAXIMUM_ATTEMPTS_REACHED` |
| Non-retryable error | `ApplicationError` | `NON_RETRYABLE_FAILURE` |
| Timed out on every attempt | `TimeoutError` | `MAXIMUM_ATTEMPTS_REACHED` |

These are the real values from the lab, not from memory.

### Cancel vs terminate

| | Cancel | Terminate |
|---|---|---|
| Can your code clean up? | Yes | **No** |
| Use for | Normal stops | Last resort, when a run is stuck and can't be cancelled |

The docs recommend cancelling unless a Workflow is stuck.

## 4. Python implementation

Read [workflows/failure_lab.py](../../temporal-learning/src/workflows/failure_lab.py), and run:

```powershell
pytest tests/test_phase05_failures.py -v -s
```

**Python detail:** `ActivityError` wraps the real exception, like Python's `raise X from Y`. Catching `ApplicationError` around an Activity call catches nothing, so catch `ActivityError` and inspect `.cause`.

```python
try:
    await workflow.execute_activity(...)
except ActivityError as error:
    if isinstance(error.cause, ApplicationError) and error.cause.type == "PaymentDeclined":
        ...
```

### The failure matrix

| Failure | Expected behaviour | Retry? | Handled where |
|---|---|---|---|
| HTTP 500 | Retry with backoff | Yes | Activity policy |
| HTTP 400 | Raise a non-retryable error | No | Activity classifies, Workflow reacts |
| Database deadlock | Retry | Yes | Activity policy |
| Invalid business input | Reject | No | Workflow or business logic |
| Worker crash | Resume by replay | Temporal | Service and history |
| Long Activity, Worker hangs | Heartbeat timeout, then retry | Depends | Activity |
| Workflow cancellation | Clean up, then cancel | No | Workflow |
| Bug in Workflow code | Task retries, run waits for a fix | Forever | You deploy a fix |
| Retries exhausted | `ActivityError` reaches the Workflow | No | Workflow decides |

## 5. Exercise

Add these to your own copy of the matrix: Temporal Service unreachable, a dependency down for a day, an expired API token, and a corrupt input file. Say which layer owns each.

## 6. Failure exercise

Incident: 400 orders show `RUNNING`. No Activities are pending. The Worker logs say `KeyError: 'shipping_address'`, and this started right after this morning's deploy.

Using the plan's six questions: what failed, where, does Temporal retry it, may a side effect have happened, must it be idempotent, and what's the right recovery?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. Why don't Workflows retry on a plain Python exception the way Activities do? What happens instead?
2. What's the difference between `ActivityError` and `ApplicationError`?
3. When would you raise `ApplicationError` in a Workflow on purpose?
4. Why are Activity retries different from Workflow retries?
5. When is terminate the right tool?

## 9. Gate

Type `/gate`. You'll investigate an unseen incident.

## 10. Next step

**Phase 6: Signals, Queries, and Updates.**
