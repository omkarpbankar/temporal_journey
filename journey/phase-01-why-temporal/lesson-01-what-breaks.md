# Lesson 1: What Breaks

## 1. Objective

By the end of this lesson you will be able to:

- explain why a normal Python program loses its place when it dies
- find the "failure windows" in a multi-step process
- explain why running it all again can make things worse

There is no Temporal code yet. First you need to feel the problem.

## 2. Mental model

### The cook with no notes

A cook is making a dish from memory, with no notes:

1. Boil the pasta.
2. Add salt.
3. Add sauce.
4. Serve.

The cook faints after adding the salt. A new cook walks in and asks, "Has the salt gone in?" Nobody knows. The only record of the progress was in the first cook's head.

The new cook has three bad choices:

- **Start again:** the dish gets double salt.
- **Skip the salt:** the dish may have no salt.
- **Throw it away:** you lose the work, but salt already in the pot can't be taken out.

### Your program is that cook

Here is a normal checkout:

```python
world.create_order(order_id, ITEM, AMOUNT)        # 1
charge_id = world.charge_card(order_id, AMOUNT)   # 2
world.reserve_inventory(order_id, ITEM)           # 3
world.send_email(order_id, ...)                   # 4
```

Your program knows it is on step 3 because of two things, both held in RAM:

- **The call stack:** which line is running right now.
- **Local variables:** values such as `charge_id`.

When the process dies, both are gone. But the outside world keeps everything that already happened. The bank does not forget the charge because your laptop lost power.

```text
YOUR PROCESS (RAM)                 THE OUTSIDE WORLD (permanent)
"I'm on step 3"                    Order saved
charge_id = "ch_1"                 Customer charged $100

        |  process dies
        v

(nothing left)                     Order saved
                                   Customer charged $100
```

That mismatch is the core problem: **the program forgets, but the world remembers.**

### The worst gap of all

Look closely at step 2. A charge involves a request and a response:

```text
Your program ----- "charge $100" -----> Bank
                                        Bank saves the charge
Your program <---- "success" ---------- Bank
```

What if the machine dies, or the network drops, after the bank saves the charge but before the "success" arrives? From your program's side, a timeout looks exactly the same as a charge that never happened. You cannot tell the two apart.

Your plan calls this question central to understanding Temporal, so keep it in mind:

> What if the machine dies right after the external system commits the change, but before my process receives the response?

## 3. Temporal model

Here is only a first glimpse, as Lessons 3 and 4 go much deeper.

Picture the cook writing each finished step in a notebook kept in a fireproof safe. When a new cook arrives, they read the notebook and carry on from there.

In Temporal, that notebook is the **Event History**, a durable, ordered log of what happened in a run. It is kept by the Temporal Service, not in your Python process. If the process running your code dies, the work is handed to another process, which uses the history to rebuild its state and continue.

Sources: [Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal), [Workflow Execution](https://docs.temporal.io/workflow-execution)

Be careful with this picture, though: a notebook records what happened only after it is written down. Keep that in mind for the failure exercise below.

## 4. Python implementation

The lab has two main files. `world.py` is the pretend outside world, saved to disk so that it survives crashes. `checkout_v1.py` is the normal checkout.

One Python detail matters here. The lab crashes with `os._exit(1)`, not `sys.exit()`. `sys.exit()` raises an exception, so `finally` blocks still run. `os._exit()` stops the process immediately, which is much closer to a power cut. This means **`try/finally` cannot protect you from a real crash**: there is no Python left to run it.

### Run the lab

Open a terminal in the lab folder:

```powershell
cd journey\phase-01-why-temporal\lab
```

**Part A: the happy path**

```powershell
python world.py reset
python checkout_v1.py ORD-1
python world.py show
```

**Part B: crash, then restart.** First, write your prediction in `my-work/failure-windows.md`. Then run:

```powershell
python world.py reset
python checkout_v1.py ORD-2 --crash-at after_charge
python world.py show
python checkout_v1.py ORD-2
python world.py show
```

The second `checkout_v1.py` line plays the part of a helpful system restarting your failed job.

**Part C: nothing crashes, and it still goes wrong.** Write your prediction first, then run:

```powershell
python world.py reset --stock 0
python checkout_v1.py ORD-3
python world.py show
```

You will see a red `OutOfStock` traceback. That is expected: it's the failure we're studying, not a bug in the lab.

Ask yourself: would retrying this fix it?

## 5. Exercise

**A. Find the failure windows.** Fill in `my-work/failure-windows.md`. Aim for at least 6 windows, and include the crash *during* a call, not only between calls.

**B. Write some code.** Finish `lab/checkout_v2.py`. It saves a checkpoint after each step, so a restart can skip finished work. Step 1 is done for you, so copy its pattern. Then test it:

```powershell
python world.py reset
python checkout_v2.py ORD-4 --crash-at after_charge_saved
python checkout_v2.py ORD-4
python world.py show
```

Then try this one, which crashes one line earlier:

```powershell
python world.py reset
python checkout_v2.py ORD-5 --crash-at after_charge
python checkout_v2.py ORD-5
python world.py show
```

Compare the two results. What did checkpoints fix, and what did they only move?

**Stretch:** read `charge_card` in `world.py`. Can you use something there to close the remaining gap, without changing `world.py`?

## 6. Failure exercise

Think about this one. You don't need code, and a guess is fine.

> You later move this checkout to Temporal. The charge step runs, and the bank saves the charge. The Worker (the process running your code) dies before it can report "success" to Temporal, so the notebook has no record of the charge.
>
> What do you think Temporal will do next? Is the customer safe from a double charge? What, if anything, must *you* still do?

## 7. Review

Pending. Tell your mentor when you've finished, or type `/check`.

## 8. Mastery questions

Answer in your own words, using a real example rather than a definition.

1. What exactly is lost when a Python process dies, and what is not lost?
2. Why can running the whole process again be worse than doing nothing?
3. Your charge request times out. What are the two possible realities, and why can't your program tell which one happened?
4. In Part C nothing crashed, and yet the system ended up wrong. Why wouldn't retries fix it? What should happen to the customer's money?
5. Where would you add `try/except` to protect against a power cut? (Careful: this is a trick question.)

## 9. Gate

Not attempted yet. You pass this lesson when your failure table, `checkout_v2.py`, and the answers above have been reviewed.

## 10. Next step

**Lesson 2: The Usual Fixes.** Cron jobs, queues, database state, and retries all exist for good reasons. We'll build on your `checkout_v2.py` and find out exactly how far each one gets you.
