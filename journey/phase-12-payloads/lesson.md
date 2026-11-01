# Phase 12: Data Conversion and Payload Design

## 1. Objective

Decide what goes through Temporal and what stays outside it.

## 2. Mental model

The Event History is a **logbook**, not a warehouse. You don't photocopy a whole shipment into the log. You write, "Shipment 42, 500 boxes, in bay 7."

That's the **claim check** pattern: store the big thing elsewhere, and carry a small ticket.

## 3. Temporal model

Everything crossing the boundary becomes a **payload**: Workflow and Activity inputs and results, Signals, Queries, and Updates. It's serialized by the data converter, usually to JSON, and **written to history forever** (within retention). Every replay reads it all again.

| Limit | Value |
|---|---|
| Single request | 2 MB |
| History transaction | 4 MB |
| Event History | 51,200 events or 50 MB. Warns at 10,240 events or 10 MB |
| SDK warning for one payload | 512 KiB, by the `payload_limits` default |

### What I measured

The same 5,000 rows, processed by the same two steps:

| Passing | History size |
|---|---|
| The whole dataset | **1,239,740 bytes** |
| A reference | **2,301 bytes** |

That's over 500 times smaller. Starting a Workflow with a 3 MB input was refused outright:

```text
RPCError: Blob data size exceeds limit.
```

### Which one do I pass?

| Data | Pass | Why |
|---|---|---|
| Order ID, status, a few fields | The value | Small, and useful in history |
| A full dataset | Object storage URI | Size |
| A customer profile | A database reference | Size, freshness, and privacy |
| Anything sensitive | A reference or a token | History is readable, and retained |
| A value the Workflow decides on | The value | The decision must replay identically |

**The trap with references:** the data behind one can change or vanish. If a Workflow reads a mutable row, two reads can differ. So write immutable objects, and include a hash. `DatasetRef` carries a `sha256`, and `ObjectStore.get_rows` refuses data that changed. Tested in `test_store_detects_changed_data`.

Also plan for retention: if the object is deleted while a run still needs it, the reference is useless.

### Newer option: External Storage

The SDK can now offload large payloads automatically, keeping a reference in history, with an S3 driver in `temporalio.contrib.aws`. It's in **Public Preview**, and the docs warn that "APIs and configuration may change before General Availability," so this course teaches the manual pattern, which works everywhere. Learn the idea first, then revisit it when it's GA.

Sources: [Limits](https://docs.temporal.io/cloud/limits), [External Storage](https://docs.temporal.io/external-storage)

## 4. Python implementation

Read [shared/payloads.py](../../temporal-learning/src/shared/payloads.py) and [workflows/payload_lab.py](../../temporal-learning/src/workflows/payload_lab.py).

```powershell
pytest tests/test_phase12_payloads.py -v -s
```

### Python details

**Type hints drive deserialization.** The converter reads your hints to rebuild objects. Without a target type, a dataclass comes back as a plain `dict`, which `test_type_hints_decide_what_you_get_back` shows. So annotate everything, including nested types.

**A single dataclass beats many arguments.** You can add a field later without changing the Workflow's signature.

**Pydantic:** the default converter doesn't fully support Pydantic. Use `temporalio.contrib.pydantic` if you want it.

### Schema evolution

Payload shape is versioned data, because history holds old payloads forever.

| Change | Safe? |
|---|---|
| Add a field **with a default** | Yes |
| Add a required field | **No.** Old data fails to decode |
| Rename a field | **No.** It's a remove plus an add |
| Change a field's type | **No** |
| Stop reading a field | Yes |

Tested in `test_adding_a_field_with_a_default_is_backward_compatible` and `test_adding_a_required_field_breaks_old_data`.

## 5. Exercise

Design payloads for `Customer`, `Order`, `Payment`, `DataPipelineRun`, and `JobStatus`. For each field, decide: inline, reference, or not in Temporal at all. Write your reasons, then compare with `shared/payloads.py`.

## 6. Failure exercise

A Workflow fans out to 1,000 Activities, each returning a 100 KB result. Do the arithmetic against the limits. What fails, and when? Now redesign it.

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. Why is passing a whole dataset bad, apart from the hard limit?
2. What must a reference guarantee to be safe?
3. Why does adding a required field break running Workflows?
4. When should data *not* be passed as a reference?
5. What happens to a dataclass if you leave off the type hint?

## 9. Gate

Type `/gate`.

## 10. Next step

**Phase 13: Observability.**
