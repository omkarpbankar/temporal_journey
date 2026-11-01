# Phase 10: Testing Temporal Applications

## 1. Objective

Test normal behaviour, failures, retries, cancellation, and replay for any Temporal feature.

## 2. Mental model

Normal tests ask, "does it work?" Temporal tests ask something harder: **"does it still work when time passes, things fail halfway, and the code changes underneath a running process?"**

You have tools for each part:

| Problem | Tool |
|---|---|
| A Workflow waits 30 days | Time-skipping server |
| An Activity calls a real bank | A mock Activity with the same name |
| Testing one Activity alone | `ActivityEnvironment` |
| Did my change break running Workflows? | `Replayer` |
| What really happens with a crash? | A real server and a real process |

## 3. Temporal model

| Environment | Clock | Use for | Doesn't support well |
|---|---|---|---|
| `start_time_skipping()` | Jumps over timers | Timers, long flows, Signals, Updates | Time stops while an Activity runs, and it only skips on its own while something awaits a result |
| `start_local()` | Real | Search and visibility, crashes, cancelling mid-Activity | Long waits |
| `ActivityEnvironment` | None | One Activity alone | Anything needing a Workflow |
| `Replayer` | None | Compatibility of code with old histories | Running anything new |

"Timers... are fast-forwarded except when Activities are running."

Source: [Testing in Python](https://docs.temporal.io/develop/python/testing-suite)

## 4. Python implementation

Run everything:

```powershell
pytest                         # all tests
pytest -m "not integration"    # the faster subset
pytest tests/test_phase10_testing_toolkit.py -v
```

### The toolkit

**Mock an Activity** by registering a fake under the real name:

```python
@activity.defn(name="compose_greeting")
async def fake_greeting(input: GreetingInput) -> str:
    return "mocked"
```

**Test an Activity directly:**

```python
ActivityEnvironment().run(activities.charge_payment, ChargeRequest("o-1", 100, "k"))
```

**Check replay against saved history.** This is the most valuable test you can have in production:

```python
history = WorkflowHistory.from_json(workflow_id, saved_json)
await Replayer(workflows=[MyWorkflow]).replay_workflow(history)
```

`tests/histories/` holds real recorded runs, and `test_saved_production_history_still_replays` replays them. Run `python scripts/record_history.py` to refresh them. A code change that would break live runs turns this test red *before* you deploy.

**Check real side effects, not just return values.** The Ledger lets a test ask, "how many charges really happened?" Without it, a double charge passes silently, because the Workflow's result looks fine.

### Your suite compared with the plan

See [TESTS.md](../../temporal-learning/TESTS.md) for how the tests count against every target in your plan.

### Fixture detail (Python)

`pytest-asyncio` normally gives each test a new event loop, but a Temporal client belongs to the loop that created it. So `pyproject.toml` runs everything on one session-wide loop:

```toml
asyncio_default_fixture_loop_scope = "session"
asyncio_default_test_loop_scope = "session"
```

### What I learned writing these tests

| Bug | How it was found |
|---|---|
| Cleanup skipped on a cancel during an Activity | A test that cancelled at the inconvenient moment |
| Tests sharing an order ID collided with an abandoned child | Unique IDs per test |
| Reading the wrong run's history after Continue-As-New | A handle with no run ID means "latest run" |
| The whole suite hung at test 160 of 180, though every file passed alone | Pairing each file with the hanging test, then each test, showed the trigger: a run with an **abandoned child** on the time-skipping server stopped it skipping time for all later tests |

**That last one is worth remembering, for three reasons.** Tests sharing a server share its state, so a test can be green and still break others. My first explanation was wrong, and bisecting found the real trigger where guessing hadn't. And I still can't say *why* that server behaves this way, only that it does, so those tests now use the real server. The suite also has a 120 second per-test timeout, so a hang fails loudly.

## 5. Exercise

Pick your own Workflow from an earlier phase and write five tests: normal, failure, retry, cancellation, and replay.

## 6. Failure exercise

This test passes. Why is it nearly worthless?

```python
async def test_checkout(env):
    result = await env.client.execute_workflow(CheckoutWorkflow.run, input, ...)
    assert result.status == "completed"
```

List what it can't detect. Then add the assertions that would catch a double charge.

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. How would you test a 30-day timer in under a second?
2. When can't you use the time-skipping server?
3. What does a replay test prove that nothing else does?
4. How do you test Worker-crash behaviour?
5. Why check side effects as well as return values?

## 9. Gate

Type `/gate`. You'll explain how to test an unseen feature in all five ways.

## 10. Next step

**Phase 11: Versioning.**
