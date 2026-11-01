# Phase 11: Versioning and Safe Evolution

## 1. Objective

Answer "Can I deploy this code change while Workflows are running?" and justify it technically.

## 2. Mental model

Remember the choose-your-own-adventure book, with your path written down: "page 1, 14, 37." Now imagine someone reprints the book with a new chapter inserted. Page 14 isn't what it was, so your written path no longer makes sense.

A Workflow that sleeps for 30 days will wake up and replay against **whatever code is deployed by then**. So a code change isn't only about the future. It rewrites the past for every running Workflow.

## 3. Temporal model

Changing this:

```python
await step_a()
await step_b()
```

to this:

```python
await step_a()
await step_c()   # new
await step_b()
```

breaks any run that has already passed `step_a`. History says "a timer came next," and the code now asks for an Activity. Here's what really happened when I deployed that change under a live run:

```text
[TMPRL1100] Nondeterminism error: Activity machine does not handle this event:
HistoryEvent(id: 11, TimerStarted)
```

**The run didn't die.** It stayed `RUNNING`, with Workflow Tasks failing. When I then deployed a compatible version, it finished normally with `["a", "b"]`. So a bad deploy stalls runs, and a good one rescues them.

### Is my change safe?

| Change | Safe for running Workflows? |
|---|---|
| Add, remove, or reorder Activities or timers | **No** |
| Change an Activity's *name* or type | **No** |
| Change which branch a run takes on replay | **No** |
| Change an Activity's *implementation* | Yes, it isn't replayed |
| Change logging or a Query | Generally yes |
| Add a Signal or Query handler | Yes |
| Change a timeout or retry value | Often safe, so verify with a replay test |

When unsure, run a replay test. It's cheap, and it's the real answer.

### Two strategies

**Worker Versioning** tags Workers with a deployment version. The docs say it "should be the default recommendation for deploying Workflow code changes in production."

- **Pinned:** a run finishes on the build it started on. Old Workers stay up until those runs drain. You don't need patches.
- **Auto-Upgrade:** a run moves to the newest version, so you must keep it replay-safe, for example with patching.

```powershell
temporal worker deployment set-current-version --deployment-name shipping --build-id build-2
```

Pinned suits short and medium Workflows. For a Workflow that lives for years, keeping an old build alive that long isn't practical, so use Auto-Upgrade plus patching, or roll over with Continue-As-New.

**Patching** branches in code:

```python
if workflow.patched("add-step-c"):
    await step_c()
```

On replay of an old run, `patched` returns False, and the old path is followed. New runs record a marker and take the new path.

Patches are removed in three deploys, waiting for old runs to finish each time:

| Step | Code | Deploy when |
|---|---|---|
| 1. Add | `if workflow.patched("x"):` | Now |
| 2. Deprecate | `workflow.deprecate_patch("x")` | No pre-patch runs remain |
| 3. Remove | Plain code | No run depends on the marker |

Sources: [Versioning in Python](https://docs.temporal.io/develop/python/versioning), [Worker Versioning](https://docs.temporal.io/worker-versioning)

## 4. Python implementation

Read [workflows/versioned.py](../../temporal-learning/src/workflows/versioned.py). It shows the same Workflow at five stages.

```powershell
pytest tests/test_phase11_versioning.py -v -s
```

| Test | Proves |
|---|---|
| `test_inserting_a_step_breaks_replay` | The unsafe change raises `NondeterminismError` |
| `test_patched_code_replays_old_history` | A patch keeps old runs valid |
| `test_deprecating_too_early_breaks_old_runs` | The order of steps matters |
| `test_bad_deploy_stalls_a_live_run_and_a_fix_rescues_it` | Recovery from a real bad deploy |
| `test_worker_versioning_keeps_a_pinned_run_on_its_own_build` | A pinned run finishes `["a","b"]` while new runs get `["a","c","b"]` |

### A bug I made here

I put `deprecate_patch` at the top of `run`, above the first Activity, while `patched` had been after it. Replay failed:

```text
Non-deprecated patch marker encountered for change add-step-c,
but there is no corresponding change command!
```

**`deprecate_patch` must sit exactly where `patched` sat.** Moving it, even by one line, is itself a nondeterministic change.

### Pydantic and payload changes

Changing a dataclass used as Workflow input is versioning too. Phase 12 covers it.

## 5. Exercise

Take your Phase 4 Workflow. Record a history, add a fraud-check step with a patch, and prove with a replay test that the old history still passes. Then write the deprecation plan: how would you know when it's safe to move to step 2?

## 6. Failure exercise

Decide whether each is safe, with reasons:

1. Renaming `charge_payment` to `take_payment`.
2. Raising a retry policy's `maximum_attempts` from 3 to 5.
3. Replacing `for x in items` with `asyncio.gather` over the same items.
4. Adding `if order.amount > 1000:` around a new Activity.
5. Changing what the `charge_payment` Activity does internally.

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. How do you deploy a new Workflow version safely?
2. Why did the bad deploy stall the run rather than fail it?
3. Pinned or Auto-Upgrade for a 3-minute checkout? For a 2-year subscription? Why?
4. Why does patch removal need three deploys?
5. How do you prove a change is safe before deploying?

## 9. Gate

Type `/gate`. You'll judge unseen diffs: "Can I deploy this?"

## 10. Next step

**Phase 12: Payloads.**
