# Phase 2: Local Temporal + Python Quickstart

## 1. Objective

Run a real Temporal application on your machine, then kill it and watch it survive.

## 2. Mental model

In Phase 1 you learned the cast. Now you'll play each part in its own terminal:

- Terminal 1 is the **Temporal Service**.
- Terminal 2 is your **Worker**.
- Terminal 3 is your **Client**.

Because they're separate processes, you can kill one and watch the others carry on. That's the whole point of this lab.

## 3. Temporal model

Here's the lifecycle your plan asks you to explain. These are the real events from a run:

```text
Workflow requested        WORKFLOW_EXECUTION_STARTED
Workflow Task scheduled   WORKFLOW_TASK_SCHEDULED
Worker receives task      WORKFLOW_TASK_STARTED
Workflow schedules step   WORKFLOW_TASK_COMPLETED, ACTIVITY_TASK_SCHEDULED
Worker runs Activity      ACTIVITY_TASK_STARTED
Result recorded           ACTIVITY_TASK_COMPLETED
Workflow continues        WORKFLOW_TASK_SCHEDULED, STARTED, COMPLETED
Run closes                WORKFLOW_EXECUTION_COMPLETED
```

There are two kinds of task. A **Workflow Task** means "run some Workflow code and tell me what you want next." An **Activity Task** means "do this one job." Your Workflow code never does the Activity itself. It produces a Command that says "please schedule this," and the Service records it and queues the task.

## 4. Python implementation

The code is in [temporal-learning/](../../temporal-learning/). Read these in order:

| File | Role |
|---|---|
| [activities/greetings.py](../../temporal-learning/src/activities/greetings.py) | The Activity |
| [workflows/hello.py](../../temporal-learning/src/workflows/hello.py) | The Workflow |
| [workers/hello_worker.py](../../temporal-learning/src/workers/hello_worker.py) | The Worker |
| [clients/hello_client.py](../../temporal-learning/src/clients/hello_client.py) | The Client |

### Setup

Your `.venv` already has everything installed. Open three PowerShell terminals, and in each one run:

```powershell
cd D:\euron\temporal
.\.venv\Scripts\Activate.ps1
cd temporal-learning
```

You don't need to install the Temporal CLI. `scripts/dev_server.py` uses the same server through the SDK. The docs' usual command is `temporal server start-dev`, and either one gives you `localhost:7233` with the UI on port 8233.

### Run it

```powershell
# Terminal 1
python scripts/dev_server.py

# Terminal 2
python -m workers.hello_worker

# Terminal 3
python -m clients.hello_client Omkar --language hi
```

Open http://localhost:8233 and click your Workflow. Find each event from section 3.

### Python details

**`@workflow.defn` and `@workflow.run` are decorators.** They attach metadata to your class so the Worker can find it. Nothing runs at import.

**`with workflow.unsafe.imports_passed_through():`** Workflow code runs in a sandbox that re-imports your Workflow file. This line says, "load these modules normally, don't re-import them." I hit this while building the lab: a script that called `asyncio.run(main())` at import time failed Worker validation with `Failed validating workflow`. That's why every runnable file has `if __name__ == "__main__":`.

**`async def` Activities must not block.** They share the Worker's event loop.

## 5. Exercise

Create `src/my_work/hello_shout.py`:

1. Write an Activity `shout(text: str) -> str` that returns the text in uppercase.
2. Write a Workflow accepting a dataclass with `name` and `times`, which calls the Activity `times` times and returns a list.
3. Add a Worker and a Client, using a new task queue.

## 6. Failure exercise

Start the slow version, which has a 30 second timer in the middle:

```powershell
python -m clients.hello_client Omkar --slow
```

**Predict first.** Then press Ctrl+C on the Worker a few seconds in, leave it off for a full minute, and restart it.

- What does the UI show while the Worker is off?
- Did the timer fire with no Worker alive?
- Did the first Activity run again? How can you prove it?

I ran this exact sequence on your machine, with `kill -9` on the Worker. The timer fired with no Worker alive, the new Worker finished the job, and the client printed `['Hello, Omkar!', 'Hola, Omkar!']`.

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. Walk through every event of a Hello run, and say which process caused each one.
2. What's the difference between a Workflow Task and an Activity Task?
3. What happens if the Client and Worker use different task queue names?
4. The Client process dies right after `start_workflow`. What happens to the run?
5. Why is `start_to_close_timeout` passed to the Activity call?

## 9. Gate

Type `/gate`. You pass when you can explain the lifecycle, your own Workflow works, and you can explain the kill-the-Worker lab.

## 10. Next step

**Phase 3: Workflow Fundamentals**, where you learn why Workflow code has rules.
