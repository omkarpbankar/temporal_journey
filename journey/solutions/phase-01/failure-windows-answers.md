# Phase 1: Failure Windows Answer Key

Fill in your own table first. Finding these yourself is the exercise.

| # | When | What failed | State of the world | If we simply run it all again | Fix without Temporal |
|---|---|---|---|---|---|
| 1 | After the order, before the charge | Process crash | Unpaid order | Duplicate order | Unique order ID, or check first |
| 2 | During the charge, request never arrived | Network | Nothing changed | Works | Retry |
| 3 | During the charge, bank committed, reply lost | Network or crash | Customer paid, program doesn't know | Double charge | Idempotency key |
| 4 | After the charge, before saving the charge ID | Process crash | Customer paid, ID lost | Double charge | Idempotency key, or look it up at the bank |
| 5 | During reserve, warehouse committed, reply lost | Network or crash | Item held, program doesn't know | Double reservation | Idempotency key |
| 6 | Warehouse says out of stock | Business failure | Paid, no item | Fails the same way each time | Refund (compensation) |
| 7 | After reserve, before the email | Process crash | Paid and reserved, customer not told | Double charge and reservation | Checkpoints plus idempotency |
| 8 | Email provider is down for an hour | Dependency outage | Paid and reserved, no email | Fails until it recovers | Retry with backoff, for hours |
| 9 | Email sent, reply lost | Network | Customer told | Duplicate email | Idempotency key, if the provider has one |
| 10 | Between steps, a deploy restarts the process | Planned restart | Depends on the moment | Any of the above | Graceful shutdown plus checkpoints |

## What the lab should have taught you

- Windows 3 and 4 are the same problem: **the world changed and you can't prove it.** Checkpoints can't help, because the crash can land between the change and the checkpoint. Only the *receiver* can solve it, by recognising a repeat.
- Window 6 is different in kind. Nothing is broken, so a retry can't help. It needs business logic that undoes earlier work.
- A key only protects the call it's on. With the sample solution, a crash at `after_charge` is safe, but `after_order`, `after_reserve`, and `after_email` still duplicate, because those calls have no key. I ran all eight crash points to confirm this.
- Window 8 shows that retries need durable state. A retry loop that lasts an hour dies with its process.
