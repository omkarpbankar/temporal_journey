# Lesson 5: Pull the Plug

## 1. Objective

Say what happens if the Worker disappears at *any* moment. This is the heart of the Phase 1 gate.

## 2. Mental model

There are only three questions, whatever the failure:

1. **What has been recorded?** That's safe forever.
2. **What's in progress?** That may or may not have happened.
3. **Who notices, and how?** The Service can't see inside a dead process. It only knows that a promised reply hasn't arrived, so **timeouts are how Temporal detects failure.**

## 3. Temporal model

Try each row yourself before reading across. Cover the right-hand columns.

| The Worker dies... | What's recorded | What Temporal does | Risk to you |
|---|---|---|---|
| Before any Worker took the run | Started, task queued | Task waits on the queue for any Worker | None |
| While running Workflow code | Everything before this task | Workflow Task times out, and another Worker replays | None, as long as the code is deterministic |
| After an Activity is scheduled, before it starts | Activity scheduled | Task waits on the queue | None |
| During an Activity, before the side effect | Activity scheduled | Start-To-Close timeout expires, then a retry | None |
| During an Activity, **after the side effect** | Activity scheduled, no result | Start-To-Close timeout expires, then a retry | **Runs twice.** Needs idempotency |
| After the Activity reported its result | Result in history | Next Worker replays and sees the result | None |
| During a 30-day timer | Timer started | Nothing is needed: the Service owns the clock | None |
| All Workers are gone for a week | Everything so far | Work waits. Runs resume when Workers return, unless a timeout you set expires first | Backlog on return |

The fifth row is the one to remember. It's the same gap you found in Phase 1, and Temporal narrows it but can't close it.

One detail: an Activity with no Start-To-Close timeout has nothing to expire, so a Worker crash wouldn't be noticed quickly. The docs "strongly recommend" setting it, because it's what the Server relies on to retry after a crash. If an Activity sets neither Start-To-Close nor Schedule-To-Close, it's rejected.

Source: [Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures)

## 4. Python implementation

You'll do this for real in Phase 2, but you can already watch the crash test from Phase 4 run:

```powershell
cd temporal-learning
pytest tests/test_phase04_worker_crash.py -v
```

It starts a Worker in a separate process, which dies with `os._exit(1)` right after the charge commits. A second Worker then finishes the job. The test proves both outcomes: 1 charge with an idempotency key and 2 without.

## 5. Exercise

Cover the table and write it again from memory in `my-work/pull-the-plug.md`. Use your own words.

## 6. Failure exercise

A Workflow is in this state:

```text
Activity A completed
Activity B started (Worker 1 running it)
```

Worker 1's network drops for 60 seconds, but the process keeps running and finishes Activity B. Start-To-Close is 30 seconds. Meanwhile, Temporal gave the retry to Worker 2.

Now two copies of Activity B have run. What happens when Worker 1 tries to report its result? What must Activity B be?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. How does the Temporal Service learn that a Worker has died?
2. What's the only way a Worker crash harms your business, if your Workflow code is correct?
3. Why does a timer need no Worker?
4. Everything is down for a day, then it all comes back. What happens to a run that was mid-Activity?
5. What happens if an Activity has no Start-To-Close timeout?

## 9. Gate

Part of the Phase 1 gate.

## 10. Next step

**Lesson 6: Gate.** Prove it from memory.
