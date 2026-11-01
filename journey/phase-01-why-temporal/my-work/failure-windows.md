# My Failure Windows

Process: create order → charge payment → reserve inventory → send confirmation.

A **failure window** is any moment where something can go wrong and leave the process half-finished. Think beyond crashes between steps. What about *during* a call? What about a step that says "no"?

The first row is an example. Add every window you can find.

| # | When exactly? | What failed? | State of the world | If we simply run it all again? | How could we fix it? |
|---|---|---|---|---|---|
| 1 | After the order is saved, before the charge | Process crashed | Unpaid order exists | A duplicate order is created | Check whether the order exists first |
| 2 | | | | | |
| 3 | | | | | |
| 4 | | | | | |
| 5 | | | | | |
| 6 | | | | | |
| 7 | | | | | |
| 8 | | | | | |

## My predictions (write these before running the lab)

**1. If I crash at `after_charge`, then run it again, what will the world look like?**

>

**2. If the warehouse is empty, what will happen to the customer's money?**

>

## What surprised me

>
