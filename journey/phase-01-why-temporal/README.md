# Phase 1: Why Temporal Exists

**Goal:** understand the problem so well that Temporal feels like the obvious answer, before you learn any of its API.

There is almost no Temporal code in this phase. That is deliberate.

| # | Lesson | The question it answers |
|---|---|---|
| 1 | What Breaks | What goes wrong when a program dies halfway through? |
| 2 | The Usual Fixes | Why don't cron, queues, DB state, and retries fully solve it? |
| 3 | Durable Execution | What does "resume where it left off" really mean? |
| 4 | The Cast | What are the Client, Service, Task Queue, Worker, Workflow, and Activity? |
| 5 | Pull the Plug | What happens if the Worker vanishes at each point? |
| 6 | Gate | Can you draw it and explain it from memory? |

## To pass this phase

You can draw this from memory and explain what happens when the Worker disappears at every point:

```text
Client
  |
  v
Temporal Service
  |
  +---- Workflow Execution
  |
  +---- Task Queue
            |
            v
         Worker
            |
            +---- Workflow Code
            |
            +---- Activity Code
```
