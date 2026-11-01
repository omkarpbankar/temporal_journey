# Phase 3: Workflow Fundamentals and Determinism

## 1. Objective

Write correct Workflows, and classify any line of Python as Workflow-safe or not.

## 2. Mental model

Your Workflow function is a script that can be **re-read at any time**. Each rerun must make the same decisions, or it won't match the notebook.

Imagine a choose-your-own-adventure book, where someone wrote down your path: "page 1, then 14, then 37." To get back to where you were, you reread from page 1 and check each choice against the record. If you now flip a coin instead of following your original choice, you'll land on the wrong page, and the record no longer makes sense.

**Determinism** means: same history in, same Commands out. Anything that can give a different answer on a rerun is dangerous: the clock, randomness, files, networks, and global variables.

## 3. Temporal model

During replay, the Commands your code produces are checked against the events in history. If they don't match, you get a nondeterminism error. Here's the real one I got by changing code under a live run:

```text
[TMPRL1100] Nondeterminism error: Activity machine does not handle this event:
HistoryEvent(id: 11, TimerStarted)
```

Read it as: "history says a timer started here, but your code asked for an Activity."

The Temporal-safe replacements record their value in history, or derive it from something that is:

| Don't use | Use |
|---|---|
| `random.random()` | `workflow.random().random()` |
| `datetime.now()` | `workflow.now()` |
| `uuid.uuid4()` | `workflow.uuid4()` |
| `time.sleep()` | `await asyncio.sleep()`, which becomes a durable timer |
| HTTP, database, or file calls | An Activity |

### What the sandbox really does

The Python SDK runs Workflows in a sandbox that blocks many unsafe calls. I tested each one:

| Inside a Workflow | Result |
|---|---|
| `random.random()` | Blocked |
| `datetime.now()` | Blocked |
| `open()` | Blocked |
| `urllib.request.urlopen()` | Blocked |
| `time.sleep()` | Blocked |
| `sqlite3.connect()` | **Not blocked. It ran and the Workflow completed** |

That last row matters. **The sandbox is a seatbelt, not a guarantee.** Passing it doesn't prove that your Workflow is correct. The docs say so too: the sandbox "is not completely isolated."

When something is blocked, the Workflow doesn't fail. The Workflow Task fails and is retried, and the run stays `RUNNING`, waiting for you to deploy a fix. Phase 5 explains why.

Sources: [Python sandbox](https://docs.temporal.io/develop/python/python-sdk-sandbox), [Workflow Execution](https://docs.temporal.io/workflow-execution)

## 4. Python implementation

Read [workflows/order_basics.py](../../temporal-learning/src/workflows/order_basics.py). It validates, waits, charges, waits for a Signal, and finishes.

```python
errors = validate_order(order)                 # Safe: pure logic
await asyncio.sleep(3600)                      # Safe: a durable timer
await workflow.execute_activity_method(...)    # Safe: asks for an Activity
await workflow.wait_condition(lambda: ..., timeout=timedelta(days=1))  # Safe: durable wait
```

**Python detail:** `asyncio.sleep` inside a Workflow isn't the normal one. The SDK runs your code on its own deterministic event loop, where sleep becomes a Timer Command. That's why `asyncio.sleep` is safe here, and `time.sleep` isn't.

**Workflow ID vs Run ID:** the Workflow ID is your business name for the process, like `order-123`. The Run ID identifies one attempt. Continue-As-New (Phase 7) keeps the Workflow ID and gets a new Run ID.

Run the failure lab:

```powershell
pytest tests/test_phase03_determinism.py -v -s
```

Read [workflows/determinism_lab.py](../../temporal-learning/src/workflows/determinism_lab.py) alongside it.

## 5. Exercise

Create `src/my_work/order_flow.py` with a Workflow that receives an order, validates it, waits, calls an Activity, waits for a condition, and completes.

Then mark **every line** with one of: Workflow-safe, Activity-only, potentially nondeterministic, or needs a Temporal API.

## 6. Failure exercise

Classify each line, and fix the unsafe ones. Be careful with the last two.

```python
if datetime.now().hour < 12: ...
price = requests.get(url).json()
key = str(uuid.uuid4())
total = sum(line.price for line in order.lines)
for tag in set(order.tags): await workflow.execute_activity(tag_order, tag, ...)
counters["orders"] += 1   # module-level dict
```

Hint for the `set` line: does Python promise an iteration order for a set of strings across processes?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. Why must Workflow code be deterministic? Use the word replay.
2. What happens during replay, step by step?
3. Why should external API calls be Activities, apart from determinism?
4. Name four Python operations that cause nondeterminism.
5. Is a Workflow that passes the sandbox always safe? Prove your answer.
6. What's the difference between a Workflow ID and a Run ID?

## 9. Gate

Type `/gate`. You'll get unseen Python to classify line by line.

## 10. Next step

**Phase 4: Activities**, where side effects belong.
