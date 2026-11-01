# Phase 19: Source Code and SDK Internals

## 1. Objective

Trace a Workflow through the SDK well enough to debug hard problems.

## 2. Mental model

So far, you've treated the SDK as a box. Open it up and there are three layers:

```text
Your Python code
      |
Python SDK (temporalio)     Workflow event loop, sandbox, converters
      |
Rust sdk-core               Polling, history handling, state machines
      |  gRPC
Temporal Service
```

## 3. Temporal model

### Watch replay happen

This is the best way to understand replay:

```powershell
python scripts/trace_lifecycle.py
```

It runs a Workflow with `max_cached_workflows=0`, so nothing is cached and each Workflow Task replays from the top. This is the real output from your machine:

```text
line 1 replaying=False
line 1 replaying=True     <- second Workflow Task, from the top
line 2 replaying=False    <- new code runs for real
line 1 replaying=True     <- third task, from the top again
line 2 replaying=True
line 3 replaying=False
line 1 replaying=True     <- fourth task
line 2 replaying=True
line 3 replaying=True
line 4 replaying=False
```

**Every line runs in full on each replay,** and Activities are the only thing skipped. Their results come from history.

With a cache, which is the default, the same Workflow ran each line once. `test_cached_worker_runs_each_line_once` proves it. The cache keeps your coroutine suspended in memory, so replay only happens when the cache misses, like after a crash or eviction. That's called **sticky execution**: the Service prefers the Worker that already has the run in memory.

### The full journey

```text
Client            start_workflow goes to the Service by gRPC
Temporal Service  writes WORKFLOW_EXECUTION_STARTED, then schedules a Workflow Task
Workflow Task     waits on the Task Queue
Python Worker     sdk-core's poller receives it, and turns history into an "activation"
Workflow code     SDK runs your coroutine until every task is blocked
                  Your await produces a Command: ScheduleActivityTask
                  sdk-core sends the Commands back, and WORKFLOW_TASK_COMPLETED is written
Activity Task     the Service schedules it
Activity Worker   a poller receives it, and your function runs
Temporal Service  the result is recorded as ACTIVITY_TASK_COMPLETED
Continuation      a new Workflow Task is scheduled, and your await returns
```

Sample history, from your machine:

```text
 1 WORKFLOW_EXECUTION_STARTED
 2 WORKFLOW_TASK_SCHEDULED
 3 WORKFLOW_TASK_STARTED
 4 WORKFLOW_TASK_COMPLETED
 5 ACTIVITY_TASK_SCHEDULED
 6 ACTIVITY_TASK_STARTED
 7 ACTIVITY_TASK_COMPLETED
 8 WORKFLOW_TASK_SCHEDULED
```

Notice events 6 and 7. `ACTIVITY_TASK_STARTED` is written when the Activity closes, along with the final attempt. That's why retried attempts don't flood history.

## 4. Python implementation

The source is in `.venv/Lib/site-packages/temporalio/`. These paths are from your installed SDK 1.35:

| File | What to look for |
|---|---|
| `worker/_workflow_instance.py` (4,200 lines) | The custom event loop. Find `activate`, `_run_once`, and how `asyncio.sleep` becomes a timer |
| `worker/_workflow.py` | Receives activations from sdk-core |
| `worker/_activity.py` | Runs Activities, heartbeats, and cancellation |
| `worker/_replayer.py` | The `Replayer` |
| `worker/workflow_sandbox/` | The import hook and restrictions |
| `converter/` | Payload converters, codecs, and failure converters |
| `client/_client.py` | Client calls and interceptors |
| `bridge/sdk-core/` | The Rust core |

### Things to find yourself

1. **Why `asyncio.sleep` is safe.** The Workflow instance is an `asyncio.AbstractEventLoop`. Find `call_later`.
2. **Why a Workflow's `asyncio.Task` order is stable.** Look at how the loop drains its ready queue.
3. **How a plain exception becomes a Workflow Task failure.** Look for `workflow_is_failure_exception`. This explains Phase 5.
4. **How the sandbox knows `open` is forbidden.** See `workflow_sandbox/_restrictions.py`.

### Debugging tools

| Problem | Tool |
|---|---|
| Is it replay-safe? | `Replayer` with a saved history |
| Step through a Workflow | `debug_mode=True` on the Worker, which disables deadlock detection so pausing in a debugger doesn't fail the task |
| See raw history | `handle.fetch_history()`, or `temporal workflow show --output json` |
| Force replay every task | `max_cached_workflows=0` |

## 5. Exercise

Pick a simple Workflow of yours, and write a technical explanation of every transition in `my-work/trace.md`, matching each to a history event. Then answer the four "find it yourself" questions, with file names and functions.

## 6. Failure exercise

Your Workers run fine, but memory grows steadily, and Workflow Task latency spikes when a Worker restarts. Using sticky execution and the cache, explain both symptoms. What setting would you look at, and what's the trade-off?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. What is an activation?
2. What does sticky execution save?
3. What happens to a cached Workflow when it's evicted?
4. Why is `ACTIVITY_TASK_STARTED` written late?
5. Trace an exception from an Activity to `ActivityError` in your Workflow.

## 9. Gate

Type `/gate`.

## 10. Next step

**The Capstone.**
