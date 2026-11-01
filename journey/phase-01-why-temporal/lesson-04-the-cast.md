# Lesson 4: The Cast

## 1. Objective

Know what each part of Temporal does, and especially **where your code runs**.

## 2. Mental model

Picture a restaurant:

| Restaurant | Temporal | Job |
|---|---|---|
| Customer placing an order | **Client** | Starts work and asks about it |
| Manager with the order book | **Temporal Service** | Remembers everything, gives out work. Never cooks |
| Ticket rail in the kitchen | **Task Queue** | Where work waits to be picked up |
| Cook | **Worker** | A process you run, which does the work |
| The recipe | **Workflow** | The plan: what happens, in what order |
| "Chop the onions" | **Activity** | One real-world action that can fail |

The most surprising part: **the manager never cooks.** Your code doesn't run on the Temporal Service. It runs in your Workers, on your machines.

## 3. Temporal model

```text
Client
  |  start_workflow
  v
Temporal Service  (stores Event History, keeps timers)
  |
  +---- Workflow Execution  (one durable run)
  |
  +---- Task Queue
            |   Workers poll: they ask for work
            v
         Worker  (your process)
            |
            +---- Workflow Code  (decides)
            |
            +---- Activity Code  (does)
```

**Temporal Service:** the backend that orchestrates runs and stores their state. "The Temporal Service schedules the work and records what happened."

**Worker:** a process you deploy, which runs Workflow and Activity code. "Workers run your code and handle your data, inside your own network."

**Task Queue:** a named queue that Workers poll. "Workers do not receive work; they ask for it." This matters more than it looks. A Worker that's busy or dead simply stops asking, so nothing gets pushed at something that can't handle it. Workflow and Activity Tasks wait in the queue until a Worker returns.

**Workflow:** your business process as code. It must be deterministic, and anything that touches the outside world belongs in an Activity.

**Activity:** one action that touches the outside world, such as an API call or a database write. Temporal supervises it with timeouts and retries.

**Where state lives:** durably, in the Event History on the Service. Workers keep a cached copy in memory for speed, and it can vanish at any time because it can be rebuilt.

Source: [Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal), [Task Queues](https://docs.temporal.io/task-queue)

## 4. Python implementation

The cast in code, trimmed from what you'll run in Phase 2:

```python
# Worker: your process
worker = Worker(client, task_queue="hello", workflows=[HelloWorkflow], activities=[compose_greeting])
await worker.run()

# Client: start a run. The Client doesn't need to know which Worker takes it.
handle = await client.start_workflow(HelloWorkflow.run, input, id="hello-1", task_queue="hello")
```

The only thing connecting them is the string `"hello"`. Nothing registers a Task Queue; it exists as soon as someone uses it. So a typo in that string doesn't raise an error. The Workflow just waits forever for a Worker that will never come.

## 5. Exercise

Close this file. In `my-work/the-cast.md`, draw the diagram from memory, then answer for each component: does it run on Temporal's side or yours? What would you lose if it disappeared?

## 6. Failure exercise

Your Temporal Service is healthy, but you've deployed no Workers at all. A Client calls `start_workflow`. Does the call succeed? What does the Workflow show as its status? What happens when you deploy a Worker an hour later?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. Does your Python code ever run on the Temporal Service?
2. Why do Workers pull work instead of having it pushed to them? Name two benefits.
3. What's the difference between a Workflow and a Workflow Execution?
4. Which component stores Workflow state, and which caches it?
5. What's the Client's relationship with the Worker? Do they ever talk directly?

## 9. Gate

Part of the Phase 1 gate.

## 10. Next step

**Lesson 5: Pull the Plug.** Now break it, at every point.
