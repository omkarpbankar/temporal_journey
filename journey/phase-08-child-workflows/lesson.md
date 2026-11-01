# Phase 8: Child Workflows and Composition

## 1. Objective

Decide what belongs in a parent, an Activity, or a Child Workflow, and what can run in parallel.

## 2. Mental model

A Workflow is like a project manager. An Activity is a single task given to a person. A **Child Workflow** is a whole sub-project given to another manager, who keeps their own notebook.

You don't make everything a sub-project. It's worth one when the work deserves its own notebook.

## 3. Temporal model

| Question | Activity | Child Workflow |
|---|---|---|
| Does it touch the outside world directly? | Yes | No, it calls Activities |
| Own Event History? | No | **Yes** |
| Can it wait days, or receive Signals? | Not really | Yes |
| Cost | Small | A whole extra run |
| Good for | One action | A multi-step process with its own lifecycle |

### When is a child worth it?

- **Its history would bloat the parent.** A fan-out to 1,000 items shouldn't sit in one history.
- **It's a separate team's process**, or it's reused elsewhere.
- **It needs its own identity:** you want to find, signal, or query it by ID.
- **It needs its own Worker pool or task queue.**

If none of these apply, use an Activity. Reaching for a child just to organise code is a common mistake. A plain Python method organises code perfectly well.

### Parent Close Policy

What happens to a child when its parent closes? The default is `TERMINATE`. Use `ABANDON` for a child that should outlive the parent, such as a notification.

Sources: [Child Workflows in Python](https://docs.temporal.io/develop/python/child-workflows)

## 4. Python implementation

Read [workflows/composed_order.py](../../temporal-learning/src/workflows/composed_order.py).

```powershell
pytest tests/test_phase08_composition.py -v
```

### Sequential vs parallel

```python
payment, inventory = await asyncio.gather(
    workflow.execute_child_workflow(PaymentWorkflow.run, order, id=...),
    workflow.execute_child_workflow(InventoryWorkflow.run, order, id=...),
    return_exceptions=True,
)
```

**Python detail:** `asyncio.gather` is safe in a Workflow, because the SDK's event loop runs it deterministically. Without `return_exceptions=True`, the first failure raises immediately while the other child keeps running, and you lose track of it.

Parallel work makes compensation harder. Now you must handle each mix: payment failed and inventory succeeded, or the reverse. Read how `ParallelOrderWorkflow` covers both.

### Fan-out with a limit

`asyncio.Semaphore` works in a Workflow, so you can run many items with only 3 at a time. See `FanOutWorkflow`.

### Child IDs: a trap I walked into

I built child IDs from the order ID, and reused one order ID across tests. Then a test failed with:

```text
temporalio.exceptions.FailureError: Workflow execution already started
```

An **abandoned** notification child from an earlier test was still open, holding that ID. So choose child IDs deliberately:

- Stable and readable: `{order_id}-payment`.
- Unique per logical run. A re-run of the same order with a new parent can collide.

## 5. Exercise

Take the order process and decide, with reasons, whether each is parent logic, an Activity, or a child:

fraud score lookup, shipping that waits days for carrier updates, a loyalty points update, sending one email, a 5,000-row refund batch, and choosing a warehouse with an `if` statement.

Then build a parallel version of your own Phase 4 Workflow.

## 6. Failure exercise

A parent starts 10,000 children in one loop, and awaits them all. What happens to the parent's history? How would you redesign it? Think about combining fan-out with Continue-As-New.

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. When should you use a Child Workflow rather than an Activity?
2. What does a parent see when a child fails?
3. What happens to a child when you cancel the parent?
4. What's the default Parent Close Policy, and when would you change it?
5. Why is `return_exceptions=True` important?
6. What makes a child Workflow ID well designed?

## 9. Gate

Type `/gate`. You'll split a large Workflow into parts, with reasons.

## 10. Next step

**Phase 9: Task Queues and Scaling.**
