# Starting Diagnostic

This is not a test you can fail. It tells your mentor where to slow down and what to skip.

Write one or two lines under each question. "I don't know" is a perfectly good answer. Please don't look anything up, because a looked-up answer hides what you need to learn.

## About you

**What is your current Python level, and have you worked with distributed systems, Airflow, AWS Step Functions, or another workflow orchestrator?**

>

## Python

**1. What is the difference between a normal function, a coroutine, and an `asyncio` Task?**

>

**2. What goes wrong if you call `time.sleep(10)` inside an `async def` function?**

>

**3. In your own words, what does a decorator do? Where would you use a retry decorator?**

>

**4. Why might you use a `@dataclass` instead of a plain `dict` to pass an order around?**

>

**5. What does `try / except / finally` guarantee, and what could stop `finally` from running?**

>

**6. How would you test, with `pytest`, a function that calls a payment API, without really calling that API?**

>

## Distributed systems

**7. You call a payment API and your request times out. Did the customer get charged? How could you find out?**

>

**8. What does "idempotent" mean? Give one operation that is idempotent and one that is not.**

>

**9. What is the difference between at-most-once and at-least-once delivery? Which one can cause duplicates?**

>

**10. A variable in your Python program holds an order's status. What happens to it when the machine loses power? Where would you keep it so it survives?**

>
