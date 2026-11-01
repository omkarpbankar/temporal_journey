# Your Temporal Journey

`plan.md` is the map. This folder holds every lesson. [temporal-learning/](../temporal-learning/README.md) holds all the code. The [root README](../README.md) is the full guide, and [document/](../document/HLD.md) has the design documents.

## What "complete" means here

Every phase now has a lesson, working code, and tests. **That means the course is ready, not that you've finished it.** Code that someone else wrote and tested teaches you nothing until you can predict it, break it, and rebuild it.

Your plan is firm on this, and so is your mentor: **a phase is finished when you pass its gate.** No gates have been passed yet, and only you can pass them.

## Start here

1. [Diagnostic](00-diagnostic.md): 10 minutes, so your mentor can pace things for you.
2. [Phase 1, Lesson 1](phase-01-why-temporal/lesson-01-what-breaks.md).
3. Type `/lesson` whenever you want to continue.

## Setup, for every session

```powershell
cd D:\euron\temporal
.\.venv\Scripts\Activate.ps1
cd temporal-learning
```

## The route

| # | Phase | Stage |
|---|---|---|
| 1 | [Why Temporal Exists](phase-01-why-temporal/README.md) | Foundations |
| 2 | [Local Quickstart](phase-02-local-quickstart/lesson.md) | Foundations |
| 3 | [Workflow Fundamentals](phase-03-workflow-fundamentals/lesson.md) | Foundations |
| 4 | [Activities](phase-04-activities/lesson.md) | Reliability |
| 5 | [Failure Semantics](phase-05-failure-semantics/lesson.md) | Reliability |
| 6 | [Signals, Queries, Updates](phase-06-signals-queries-updates/lesson.md) | Features |
| 7 | [Long-Running Workflows](phase-07-long-running/lesson.md) | Features |
| 8 | [Child Workflows](phase-08-child-workflows/lesson.md) | Features |
| 9 | [Task Queues and Scaling](phase-09-task-queues-scaling/lesson.md) | Scale |
| 10 | [Testing](phase-10-testing/lesson.md) | Production |
| 11 | [Versioning](phase-11-versioning/lesson.md) | Production |
| 12 | [Payloads](phase-12-payloads/lesson.md) | Production |
| 13 | [Observability](phase-13-observability/lesson.md) | Production |
| 14 | [Security](phase-14-security/lesson.md) | Production |
| 15 | [Cloud and Self-Hosted](phase-15-cloud-vs-self-hosted/lesson.md) | Production |
| 16 | [AWS](phase-16-aws/lesson.md) | Applied |
| 17 | [Data Engineering](phase-17-data-engineering/lesson.md) | Applied |
| 18 | [Advanced Patterns](phase-18-advanced-patterns/lesson.md) | Architect |
| 19 | [SDK Internals](phase-19-sdk-internals/lesson.md) | Architect |
| - | [Capstone](capstone/README.md) | Final |
| - | [Final Assessment](final-assessment/README.md) | Final |

## How a session works

1. **Recap:** a few quick questions on the last concept.
2. **Learn:** one concept, starting from a plain-language picture.
3. **Do:** write or run code. Your own code goes in `temporal-learning/src/my_work/`.
4. **Break it:** cause the failure on purpose. Predict first.
5. **Prove it:** explain it in your own words, using real examples.

Saying "I don't know" is useful, because it shows exactly what to teach next.

## Commands

Type these in the Claude Code chat:

| Command | What it does |
|---|---|
| `/lesson` | Continue with the next step |
| `/check` | Get your work reviewed |
| `/gate` | Take the current phase's mastery test |
| `/progress` | See where you are |

You can also ask questions in normal language at any time.

## Try not to peek

`solutions/` has answers for the Phase 1 exercise, and the finished code shows how each phase was built. Attempt each exercise first. The struggle is where the learning happens.
