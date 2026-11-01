# Learn Temporal with Python: From Zero to Production

A hands-on course for mastering [Temporal](https://docs.temporal.io/develop/python) with Python. It takes you from "why does this exist?" to designing and operating production systems, through 19 phases, a capstone project, and a final assessment.

Every lesson comes with code you can run, break, and rebuild, and an AI mentor in Claude Code teaches it with you one concept at a time.

**New here?** Do the [Quick start](#quick-start), then read [How to learn with this repo](#how-to-learn-with-this-repo).

## Contents

- [What's in this repo](#whats-in-this-repo)
- [Current status](#current-status)
- [Quick start](#quick-start)
- [Repository layout](#repository-layout)
- [How to learn with this repo](#how-to-learn-with-this-repo)
- [Your first week, step by step](#your-first-week-step-by-step)
- [Phase-by-phase roadmap](#phase-by-phase-roadmap)
- [Experiments worth running](#experiments-worth-running)
- [Using the tests as a teacher](#using-the-tests-as-a-teacher)
- [Capstone and final assessment](#capstone-and-final-assessment)
- [Design documents](#design-documents)
- [Cheat sheet](#cheat-sheet)
- [Troubleshooting](#troubleshooting)
- [What hasn't been verified](#what-hasnt-been-verified)
- [References](#references)

## What's in this repo

| Part | Location | What it gives you |
|---|---|---|
| The curriculum | [plan.md](plan.md) | The full roadmap, and the rules of the course |
| Lessons | [journey/](journey/README.md) | 24 lessons, with exercises and mastery questions |
| Code and tests | [temporal-learning/](temporal-learning/README.md) | Working code for every phase, and 180 test cases |
| Design documents | [document/](document/HLD.md) | High-level and low-level design |
| AI mentor | [.claude/](.claude/CLAUDE.md) | Teaching rules, slash commands, and your progress record |

## Current status

| Area | Status |
|---|---|
| Lessons, labs, and code for all phases | Ready |
| Test suite | 180 of 180 passing, on two consecutive full runs |
| Capstone reference build | Ready, and run end to end |
| **Your progress** | **Phase 1, Lesson 1. No gates passed yet** |

The course being ready doesn't mean it's finished. **A phase is finished when you pass its gate.** The code here is a reference to learn from, not a replacement for doing the work.

## Quick start

You need Windows with PowerShell. Python 3.11 and all dependencies are already installed in `.venv`.

### 1. Activate the environment

Do this in every new terminal:

```powershell
cd D:\euron\temporal
.\.venv\Scripts\Activate.ps1
cd temporal-learning
```

### 2. Check that everything works

```powershell
pytest -m "not integration"
```

You should see `100 passed` in about 40 seconds.

### 3. Run your first Workflow

Open three terminals, and run the activation steps in each.

```powershell
# Terminal 1: the Temporal Service, with its Web UI
python scripts/dev_server.py

# Terminal 2: a Worker
python -m workers.hello_worker

# Terminal 3: a Client that starts a Workflow
python -m clients.hello_client Omkar --language hi
```

You should see `greeting='Namaste, Omkar!'`. Open http://localhost:8233 and click your Workflow to see its Event History.

### 4. Start learning

Fill in the [diagnostic](journey/00-diagnostic.md), then type `/lesson` in the Claude Code chat.

## Repository layout

```text
temporal/
├── README.md                  You are here
├── plan.md                    The curriculum
├── document/
│   ├── HLD.md                 High-level design
│   └── LLD.md                 Low-level design
├── journey/                   Everything you read and fill in
│   ├── 00-diagnostic.md       Start here
│   ├── phase-01-why-temporal/ Six lessons, a lab, and your answers
│   ├── phase-02-.../lesson.md One lesson per later phase
│   ├── capstone/
│   ├── final-assessment/
│   └── solutions/             Phase 1 answers. Try it yourself first
├── temporal-learning/         Everything that runs
│   ├── src/
│   │   ├── workflows/         What should happen, deterministically
│   │   ├── activities/        Everything that touches the outside world
│   │   ├── workers/           Processes that run Workflows and Activities
│   │   ├── clients/           Processes that start Workflows
│   │   ├── shared/            Models, connection, encryption, diagnosis
│   │   ├── capstone/          The final project
│   │   └── my_work/           YOUR code goes here
│   ├── tests/                 One file per phase, plus the capstone
│   ├── scripts/               Dev server, tracing, diagnostics
│   ├── observability/         Prometheus, alerts, Grafana dashboard
│   └── deploy/                Dockerfile and compose file
├── .claude/                   Mentor rules, commands, and your progress
└── .venv/                     Python environment
```

You can ignore `src/temporal_learning.egg-info/`, `__pycache__/`, and `.mypy_cache/`. Tools generate them.

## How to learn with this repo

### The one rule

**Predict first, then run.** Before you run anything, say out loud what you expect, including what happens on failure. A wrong prediction teaches you more than a correct run.

Reading finished code feels like learning, but it mostly teaches you to recognise things. The aim is to look at a business process and design it yourself. So treat the code as something to take apart.

### The loop for every phase

| Step | What you do | Where |
|---|---|---|
| 1. Read | Read the lesson up to the code section. Stop there | `journey/phase-NN-.../lesson.md` |
| 2. Predict | Answer the failure question in your head | Lesson section 6 |
| 3. Read code | Read the listed files, in the listed order | `temporal-learning/src/` |
| 4. Run | Run the tests with `-v -s`, then the scripts | `temporal-learning/` |
| 5. Break | Change something, and predict what will fail | See below |
| 6. Build | Do the exercise without copying | `src/my_work/` |
| 7. Review | Type `/check` | Claude Code |
| 8. Prove | Answer the mastery questions in your own words | Lesson section 8 |
| 9. Gate | Type `/gate`. If you fail, you get a new example, and try again | Claude Code |

### Five ways to break the code

Pick any Workflow, and try these:

1. **Add a line** in the middle, then run a replay test. Did you break running Workflows?
2. **Remove an idempotency key**, then run a crash test. How many charges happen?
3. **Remove a timeout**, and see what the Service does.
4. **Kill the Worker** at a moment of your choosing, and explain the result from history.
5. **Replace `workflow.now()`** with `datetime.now()`, and read the error.

Undo your changes afterwards, or make them on a copy in `src/my_work/`.

### Three questions to ask of every line

Your plan calls these out, and they're worth making a habit:

1. What happens if the Worker crashes at this exact line?
2. Can this Workflow replay deterministically?
3. What happens if this Activity executes twice?

### Daily routine (60 to 90 minutes)

| Minutes | Activity |
|---|---|
| 10 | Review yesterday's concepts. `/lesson` opens with a recap |
| 20 | Learn one new concept |
| 30 to 45 | Write code |
| 10 | Break the code on purpose |
| 10 | Answer mastery questions |

### Working with your mentor

Type these in the Claude Code chat:

| Command | Use it when |
|---|---|
| `/lesson` | You want to continue. It picks up where you stopped |
| `/check` | You've finished an exercise, and want it reviewed |
| `/gate` | You're ready to prove a phase |
| `/progress` | You want to see where you are |

You can also ask anything in plain language. Some useful ones:

- "Explain replay with a different analogy."
- "Quiz me on Activities, and don't give me the answers."
- "Why does this test fail?"
- "Give me a harder version of this exercise."

Expect the mentor to push back. It's set up to ask for real examples, not definitions. If you say "Temporal retries it," it will ask what exactly is retried, under what policy, and what happens to the side effect.

### When you're stuck

1. Say "I don't know" honestly. It shows exactly what to teach next.
2. Look at the history in the UI. It usually tells you what really happened.
3. Run `python scripts/diagnose.py <workflow id>` on a stuck run.
4. Ask for a hint, not the answer.

### Suggested pace

Twelve weeks at 60 to 90 minutes a day. These aren't deadlines: stay on a phase until you pass its gate.

| Week | Phases | You finish with |
|---|---|---|
| 1 | Prerequisites, 1 | Architecture notes |
| 2 | 2 | Your first Workflow |
| 3 | 3 | Determinism lab |
| 4 | 4 | A reliable API integration |
| 5 | 5 | Failure matrix |
| 6 | 6 | An interactive order Workflow |
| 7 | 7 | A subscription Workflow |
| 8 | 8, 9 | A multi-Worker system |
| 9 | 10, 11 | A production test suite |
| 10 | 12, 13, 14 | Dashboard and security design |
| 11 | 15, 16, 17 | A durable data pipeline |
| 12 | 18, 19, Capstone | A production-grade system |

## Your first week, step by step

**Day 1: find out where you stand.** Complete the [diagnostic](journey/00-diagnostic.md) without looking anything up, then type `/check`. Your mentor adjusts the pace from your answers. Then read [Lesson 1](journey/phase-01-why-temporal/lesson-01-what-breaks.md) up to the lab.

**Day 2: feel the problem.** Write your predictions in [failure-windows.md](journey/phase-01-why-temporal/my-work/failure-windows.md). Then run the lab, which needs no Temporal at all:

```powershell
cd journey\phase-01-why-temporal\lab
python world.py reset
python checkout_v1.py ORD-2 --crash-at after_charge
python world.py show
python checkout_v1.py ORD-2
python world.py show
```

You've just double-charged a customer. Find out why.

**Day 3: try to fix it yourself.** Finish `lab/checkout_v2.py`, and test each crash point. Notice how much bookkeeping you need, and which crash still gets through.

**Day 4: the usual fixes.** Do Lesson 2. See why cron, queues, and status tables each stop short.

**Day 5: durable execution.** Do Lessons 3 and 4. Draw the architecture from memory.

**Day 6: pull the plug.** Do Lesson 5, covering the table and predicting each row. Then run:

```powershell
cd temporal-learning
pytest tests/test_phase04_worker_crash.py -v
```

**Day 7: gate.** Review [Lesson 6](journey/phase-01-why-temporal/lesson-06-gate.md), then type `/gate`.

## Phase-by-phase roadmap

"Key idea" is the single thing to take from each phase. Run the listed tests from `temporal-learning/`, and add `-v -s` to see details.

### Foundations

| Phase | Key idea | Main code | Tests |
|---|---|---|---|
| [1 Why Temporal](journey/phase-01-why-temporal/README.md) | The program forgets, but the world remembers | `journey/.../lab/` | The lab |
| [2 Quickstart](journey/phase-02-local-quickstart/lesson.md) | Your code runs in your Workers, never on the Service | `workflows/hello.py` | `test_phase02_hello.py` |
| [3 Determinism](journey/phase-03-workflow-fundamentals/lesson.md) | Workflow code is re-read on replay, so it must decide the same way | `workflows/order_basics.py`, `determinism_lab.py` | `test_phase03_determinism.py` |

### Reliability

| Phase | Key idea | Main code | Tests |
|---|---|---|---|
| [4 Activities](journey/phase-04-activities/lesson.md) | An Activity can run twice, so design for it | `activities/commerce.py`, `workflows/checkout.py` | `test_phase04_*.py` |
| [5 Failures](journey/phase-05-failure-semantics/lesson.md) | A Python bug pauses a Workflow. `ApplicationError` ends it | `workflows/failure_lab.py` | `test_phase05_failures.py` |

### Features

| Phase | Key idea | Main code | Tests |
|---|---|---|---|
| [6 Messages](journey/phase-06-signals-queries-updates/lesson.md) | Signal is a note, Query is a look, Update is a phone call | `workflows/interactive_order.py` | `test_phase06_interactive.py` |
| [7 Long-running](journey/phase-07-long-running/lesson.md) | History has limits, and a cancel takes several forms | `workflows/subscription.py` | `test_phase07_*.py` |
| [8 Composition](journey/phase-08-child-workflows/lesson.md) | A child is for work that deserves its own history | `workflows/composed_order.py` | `test_phase08_composition.py` |

### Scale and production

| Phase | Key idea | Main code | Tests |
|---|---|---|---|
| [9 Scaling](journey/phase-09-task-queues-scaling/lesson.md) | Queues isolate failure, and overload shows up as latency | `workflows/routed_order.py`, `workers/specialized.py` | `test_phase09_routing.py` |
| [10 Testing](journey/phase-10-testing/lesson.md) | Check real side effects, and replay real histories | `tests/`, `scripts/record_history.py` | `test_phase10_testing_toolkit.py` |
| [11 Versioning](journey/phase-11-versioning/lesson.md) | A code change rewrites the past for every open run | `workflows/versioned.py` | `test_phase11_versioning.py` |
| [12 Payloads](journey/phase-12-payloads/lesson.md) | History is a logbook, not a warehouse | `shared/payloads.py`, `workflows/payload_lab.py` | `test_phase12_payloads.py` |
| [13 Observability](journey/phase-13-observability/lesson.md) | Metrics say that, history says what, logs say why | `shared/diagnose.py`, `observability/` | `test_phase13_observability.py` |
| [14 Security](journey/phase-14-security/lesson.md) | Encrypt before data leaves your process, and know what stays visible | `shared/encryption.py`, `scripts/codec_server.py` | `test_phase14_security.py` |
| [15 Deployment models](journey/phase-15-cloud-vs-self-hosted/lesson.md) | Either way, you run your own Workers | `shared/connection.py` | `test_phase15_connection.py` |

### Applied and architect level

| Phase | Key idea | Main code | Tests |
|---|---|---|---|
| [16 AWS](journey/phase-16-aws/lesson.md) | Each service needs its own idempotency approach | `activities/aws_pipeline.py` | `test_phase16_aws.py` |
| [17 Data engineering](journey/phase-17-data-engineering/lesson.md) | Temporal doesn't replace Airflow. Know when to use each | `workflows/data_pipeline.py` | `test_phase17_data_pipeline.py` |
| [18 Patterns](journey/phase-18-advanced-patterns/lesson.md) | You can't roll back the world, only do new work that undoes it | `workflows/fulfillment_platform.py`, `patterns.py` | `test_phase18_platform.py` |
| [19 Internals](journey/phase-19-sdk-internals/lesson.md) | Every line reruns on replay, and only Activities are skipped | `workflows/internals_lab.py`, `scripts/trace_lifecycle.py` | `test_phase19_internals.py` |

### Mastery levels

Use these from your plan to judge your progress:

| Level | You can | Roughly after |
|---|---|---|
| 1 Beginner | Run Temporal, and build a Workflow, Activity, Worker, and Client | Phase 2 |
| 2 Intermediate | Use retries, timeouts, timers, Signals, and Queries, and explain determinism | Phases 3 to 6 |
| 3 Advanced | Design long-running Workflows and scale Workers | Phases 7 to 11 |
| 4 Production Engineer | Deploy, observe, secure, and diagnose | Phases 12 to 16 |
| 5 Architect | Choose technologies, design at scale, and review designs | Phases 17 to 19, and the capstone |

## Experiments worth running

Each takes minutes, and each teaches something that's hard to get from reading.

### Kill a Worker in the middle of a run

With the dev server and Worker running:

```powershell
python -m clients.hello_client Omkar --slow
```

Press Ctrl+C on the Worker within a few seconds, and wait a full minute. Then restart it. The 30 second timer fires with no Worker alive, and the new Worker finishes the job.

### Watch replay happen

```powershell
python scripts/trace_lifecycle.py
```

With the cache turned off, you'll see each line of the Workflow rerun on every Workflow Task, printing `replaying=True`. Only new lines run for real.

### See a double charge, then prevent it

```powershell
pytest tests/test_phase04_worker_crash.py -v
```

A real process is killed right after the bank commits. It gives 1 charge with an idempotency key and 2 without.

### Simulate a year in seconds

```powershell
pytest tests/test_phase07_subscription.py -v -s
```

Look for `360 simulated days took`. It's about 1.3 seconds.

### Deploy a bad change under a live Workflow

```powershell
pytest tests/test_phase11_versioning.py -v -s
```

Look for the `[TMPRL1100] Nondeterminism error`. The run stalls instead of failing, and a fixed deploy rescues it.

### Compare payload sizes

```powershell
pytest tests/test_phase12_payloads.py -v -s
```

The same 5,000 rows cost about 1.2 MB of history by value, and about 2 KB by reference.

## Using the tests as a teacher

Each test is a small, runnable claim about Temporal. Test names read like sentences, such as `test_lost_response_without_idempotency_charges_twice`.

```powershell
pytest -m "not integration"                  # Fast subset, about 40 seconds
pytest                                       # Everything, about 3.5 minutes
pytest tests/test_phase06_interactive.py -v  # One phase
pytest -k "cancel" -v                        # Everything about cancellation
pytest tests/test_phase05_failures.py -v -s  # Show printed observations
```

Tests print lines starting with `OBSERVED` when there's a measured result worth seeing, so use `-s`.

Try this way of studying: read a test's name, predict how it works, write your own version, then compare it with the real one.

[TESTS.md](temporal-learning/TESTS.md) maps the suite to the targets in your plan.

## Capstone and final assessment

**[The capstone](journey/capstone/README.md)** is a data-processing platform: an API, a Workflow with a Child Workflow, three Worker pools, human approval, cleanup on failure, and chaos tests. A reference build exists, so your work is different from building it from nothing:

1. Trace one request through the UI, event by event.
2. Kill Workers by hand, predicting each outcome.
3. Extend it with two of: real PostgreSQL, deployment to AWS, per-tenant queues, or a new patched step.
4. Defend each design decision to your mentor.

**[The final assessment](journey/final-assessment/README.md)** comes after the capstone: 25 concept questions, 5 broken apps, a coding task, 10 failure scenarios, an architecture design, and a code review. Don't read `.claude/assessment/`, because it holds the answer key.

## Design documents

| Document | Read it for | Best time to read |
|---|---|---|
| [HLD](document/HLD.md) | The big picture: components, flows, and why it's built this way | After Phase 2, then again after Phase 9 |
| [LLD](document/LLD.md) | Exact contracts: Workflows, Activities, timeouts, keys, and states | As a reference during each phase |

## Cheat sheet

### Everyday commands

| Task | Command |
|---|---|
| Activate | `.\.venv\Scripts\Activate.ps1` |
| Start Temporal and the UI | `python scripts/dev_server.py` |
| Open the UI | http://localhost:8233 |
| Run a Worker | `python -m workers.hello_worker` |
| Start a Workflow | `python -m clients.hello_client Omkar` |
| Why is this run stuck? | `python scripts/diagnose.py <workflow id>` |
| Watch replay | `python scripts/trace_lifecycle.py` |
| Refresh saved histories | `python scripts/record_history.py` |

### What goes where

| If the code... | Put it in |
|---|---|
| Decides what happens next | A Workflow |
| Calls an API, database, or file | An Activity |
| Is one action that can fail | An Activity |
| Is a multi-step process with its own lifecycle | A Child Workflow |
| Needs the time, a random value, or a UUID in a Workflow | `workflow.now()`, `workflow.random()`, `workflow.uuid4()` |
| Needs to wait in a Workflow | `asyncio.sleep` or `workflow.wait_condition` |

### Which message?

| You want to | Use |
|---|---|
| Notify a Workflow, no reply needed | Signal |
| Read state without changing it | Query |
| Change state, and get a validated reply | Update |

## Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| `Activate.ps1 cannot be loaded` | PowerShell's script policy | `Set-ExecutionPolicy -Scope Process RemoteSigned`, then activate again |
| `ModuleNotFoundError: workflows` | The venv isn't active | Activate it. Your prompt should show `(.venv)` or similar |
| `pytest` isn't recognised | Same cause | Activate it |
| Client can't connect to `localhost:7233` | The dev server isn't running | Start `python scripts/dev_server.py` |
| Dev server won't start | Port 7233 is in use | Stop the other Temporal server |
| A Workflow stays `Running` and nothing happens | No Worker, or a mistyped task queue name | Run `diagnose.py`, then check the queue names match |
| Client waits forever | The Worker isn't running | Start the Worker. The Workflow is safe and waiting |
| `[TMPRL1100] Nondeterminism error` | Workflow code changed under a live run | Restore compatible code, or add `workflow.patched`. Don't terminate the run |
| `Cannot access ... from inside a workflow` | The sandbox blocked an unsafe call | Move it to an Activity, or use the `workflow.*` version |
| `Failed validating workflow` | Code runs at import time | Put startup code under `if __name__ == "__main__":` |
| `Workflow execution already started` | That Workflow ID is in use | Use a unique ID, or stop the old run |
| Run stuck after adding a Search Attribute | It isn't registered on the server | Register it before deploying |
| A test fails with a timeout | Each test has a 120 second limit | Read which test it was, and run it alone |
| First test run is slow, or fails offline | Temporal's test servers are being downloaded | Connect to the internet once. They're cached after that |
| `/lesson` isn't recognised | Claude Code hasn't loaded the commands | Restart the session once |

## What hasn't been verified

This course is honest about its limits. These weren't run:

| Item | Why | How it was covered instead |
|---|---|---|
| Self-hosted Temporal cluster | Running the external Docker sample was blocked in the build environment | Steps are in [Phase 15](journey/phase-15-cloud-vs-self-hosted/lesson.md) |
| Temporal Cloud | Needs an account | Connection settings are tested. A real connection isn't |
| Real AWS | No account was used | Real `boto3` calls against moto, an in-process fake |
| mTLS | Needs certificates and a configured server | The code is from the docs, and not run |
| Prometheus and Grafana | Never loaded into either tool | Files validated, and metric names checked against a real Worker |
| Docker compose stack | Never started | Image built, imports checked, and compose file validated |
| Performance under load | Out of scope | Capacity is taught with arithmetic, and small measurements |

All tests ran on the single-binary dev server and the time-skipping test server.

## References

- [Temporal Python SDK docs](https://docs.temporal.io/develop/python)
- [Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal)
- [Testing in Python](https://docs.temporal.io/develop/python/testing-suite)
- [Worker Versioning](https://docs.temporal.io/worker-versioning)
- [SDK metrics](https://docs.temporal.io/references/sdk-metrics)
- [Limits](https://docs.temporal.io/cloud/limits)

Built against Temporal Python SDK 1.35.0. When this course and the current docs disagree, **the docs win**.
