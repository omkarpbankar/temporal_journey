# Lesson 3: Durable Execution

## 1. Objective

Explain what "resume where it left off" really means, without using the word "magic."

## 2. Mental model

Go back to the cook. Now every finished step goes in a notebook kept in a safe:

```text
1. Boiled pasta: done
2. Added salt: done
```

When a new cook arrives, they don't guess. They start the recipe from line 1 and read along with the notebook:

- "Boil the pasta." The notebook says it's done, so they don't touch the stove. They just note the result.
- "Add salt." Also done, so they skip it.
- "Add sauce." The notebook has nothing, so this is real work. They do it.

Reading along like this is called **replay**. By the end of the notebook, the new cook knows everything the first cook knew. **Resuming is a rebuild**, not a teleport of the old process.

## 3. Temporal model

The notebook is the **Event History**. The docs define Durable Execution by its result: "Once a Workflow starts, it runs to completion, whether that takes a second or a year."

Here's what really happens in the checkout:

1. Your Workflow code reaches `charge_payment`. It doesn't call the bank. It tells the Service, "please schedule this Activity."
2. The Service records that and queues the task.
3. A Worker runs the Activity, and the result is recorded in history.
4. If the Worker dies, a new Worker gets the history and reruns your Workflow function from the top.
5. When the code reaches `charge_payment` again, the SDK sees the result in history and returns it instantly. **The bank isn't called again.**
6. The code reaches a line with no history, and real work continues.

Two consequences follow, and much of this course is about them:

**Workflow code must make the same decisions on every rerun.** If it took a different branch the second time, it wouldn't match the notebook. That's called determinism, and Phase 3 is all about it.

**The notebook records what's finished, not what's in progress.** Now answer the failure exercise from Lesson 1.

### The Lesson 1 failure exercise, answered

The bank saved the charge, the Worker died, and history has no result. Temporal retries the Activity, because it can't know the bank committed. So:

- If the Activity has no idempotency key: **the customer is charged twice.**
- If it has one: the bank recognises the repeat and returns the first charge.

This isn't a guess. `tests/test_phase04_worker_crash.py` kills a real process at that exact moment, and observes 2 charges without a key and 1 with a key.

**Temporal doesn't remove this gap. It guarantees the retry happens, and leaves idempotency to you.** Anyone who says "Temporal gives you exactly-once side effects" is wrong.

Sources: [Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal), [Workflow Execution](https://docs.temporal.io/workflow-execution), [Activities](https://docs.temporal.io/activities)

## 4. Python implementation

A minimal sketch, to see the shape. You'll run real code in Phase 2.

```python
@workflow.defn
class CheckoutWorkflow:
    @workflow.run
    async def run(self, order: Order) -> str:
        charge_id = await workflow.execute_activity(charge_payment, order, ...)
        await workflow.execute_activity(reserve_inventory, order, ...)
        await workflow.execute_activity(send_email, charge_id, ...)
        return charge_id
```

Compare this with your `checkout_v2.py`. There's no `done` list, no `save_state`, and `charge_id` is a plain variable. It "survives" because replay rebuilds it from history.

Python detail: `await` here isn't waiting on a network call. It suspends your coroutine until the SDK has a result, from a live Activity or from history. Your code can't tell which, and that's the design.

## 5. Exercise

In `my-work/durable-execution.md`, take this history and write what a new Worker does at each line of the code above:

```text
Started
Activity charge_payment completed: "ch_1"
Activity reserve_inventory scheduled
```

## 6. Failure exercise

In the code above, a developer adds this as the first line of `run`:

```python
if random.random() < 0.5:
    await workflow.execute_activity(fraud_check, order, ...)
```

The first run skips `fraud_check`. The Worker crashes. The replay rolls differently and wants to call `fraud_check`. What's wrong, and what do you think Temporal does?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. Where is `charge_id` stored while the Workflow runs? Careful: there are two answers.
2. On replay, is `charge_payment` called again? How do you know?
3. Why can't Temporal make Activities exactly-once?
4. What does "resume" mean physically? Which process does the work?
5. A Workflow sleeps for 30 days. Is any process running for those 30 days?

## 9. Gate

Part of the Phase 1 gate.

## 10. Next step

**Lesson 4: The Cast.** Who actually does all of this?
