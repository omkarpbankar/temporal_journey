# Temporal Python --- From Zero to Production/Pro

> **Purpose:** A hands-on learning roadmap for mastering Temporal with
> Python, from first principles to production-grade workflow
> architecture.
>
> **Primary reference:** Temporal Python SDK documentation ---
> https://docs.temporal.io/develop/python.md
>
> **How to use this file:** Give this `plan.md` to an AI coding agent
> and tell it to act as your Temporal mentor. The agent should teach one
> phase at a time, make you write code, review your work, create
> exercises, and refuse to move forward until the mastery gate is
> passed.

------------------------------------------------------------------------

## 0. Learning Contract

### Target outcome

By the end of this plan, I should be able to:

-   Explain Temporal's execution model without relying on memorized
    terminology.
-   Build Temporal applications in Python from scratch.
-   Design deterministic Workflows correctly.
-   Implement Activities with retries, timeouts, heartbeats, and
    idempotency.
-   Run and operate Workers.
-   Use Signals, Queries, Updates, Child Workflows, Timers, and
    cancellation correctly.
-   Understand Workflow history, replay, retries, and failure recovery.
-   Design long-running and stateful business processes.
-   Test Temporal applications properly.
-   Version and safely evolve Workflow code.
-   Understand task queues, Worker concurrency, routing, and scaling.
-   Build production-grade observability and operational practices.
-   Deploy Temporal applications using Temporal Cloud or self-hosted
    Temporal.
-   Integrate Temporal with databases, APIs, queues, AWS services, and
    data-engineering pipelines.
-   Diagnose common Temporal production failures.
-   Review another engineer's Temporal design and identify correctness
    problems.
-   Make architecture decisions involving Temporal rather than simply
    writing Temporal code.

### Learning principles

1.  **Understand before memorizing.**
2.  **Code every major concept.**
3.  **Break things deliberately.**
4.  **Observe Workflow history and Worker behavior.**
5.  **Repeat concepts using progressively harder projects.**
6.  **Always ask: "What happens if the Worker crashes here?"**
7.  **Always ask: "Can this Workflow replay deterministically?"**
8.  **Always ask: "What happens if this Activity executes twice?"**
9.  **Prefer production reasoning over toy examples.**

------------------------------------------------------------------------

# 1. Prerequisites

## Python

I should be comfortable with:

-   Python functions and classes
-   decorators
-   type hints
-   dataclasses
-   exceptions
-   context managers
-   async/await
-   `asyncio`
-   packages and virtual environments
-   `pytest`
-   logging
-   HTTP APIs
-   JSON serialization
-   basic concurrency concepts

### Required Python exercises

-   [ ] Write an async function that calls an HTTP API.
-   [ ] Create and use an async task with `asyncio`.
-   [ ] Write a retry decorator.
-   [ ] Write a small class with typed methods.
-   [ ] Write pytest unit tests.
-   [ ] Explain the difference between a coroutine, Task, and normal
    function.
-   [ ] Explain why blocking synchronous code inside async code can be
    problematic.

## Distributed systems

Learn enough to understand:

-   process failure
-   network failure
-   retries
-   duplicate execution
-   idempotency
-   eventual consistency
-   timeouts
-   message delivery semantics
-   durable state
-   fault tolerance
-   horizontal scaling

### Distributed-systems thought experiment

For every operation, answer:

> "What if the machine dies immediately after the external system
> commits the change but before my process receives the response?"

This question is central to understanding why Temporal exists.

------------------------------------------------------------------------

# 2. Phase 1 --- Why Temporal Exists

## Goal

Understand the problem before learning the API.

## Topics

-   Traditional application orchestration
-   Cron jobs
-   Airflow-style orchestration
-   queues
-   application state machines
-   retries in application code
-   database-backed job state
-   distributed failures
-   Temporal's durable execution model
-   Workflows versus normal application code

## Questions I must be able to answer

-   What problem does Temporal solve?
-   Why isn't a normal Python process sufficient?
-   Why aren't retries alone enough?
-   What is durable execution?
-   What does "resume exactly where it left off" mean?
-   Where does Workflow state live?
-   What happens when a Worker crashes?
-   What is a Temporal Service?
-   What is a Worker?
-   What is a Workflow?
-   What is an Activity?
-   What is a Task Queue?

## Exercise

Design a payment process without Temporal:

1.  Create order.
2.  Charge payment.
3.  Reserve inventory.
4.  Send confirmation.

Then identify every failure window.

## Mastery gate

I can draw the architecture:

``` text
Client
  |
  v
Temporal Service
  |
  +---- Workflow Execution
  |
  +---- Task Queue
            |
            v
         Worker
            |
            +---- Workflow Code
            |
            +---- Activity Code
```

and explain what happens when the Worker disappears at every point.

------------------------------------------------------------------------

# 3. Phase 2 --- Local Temporal + Python Quickstart

## Goal

Run a complete Temporal application locally.

## Topics

-   Python SDK installation
-   Temporal development server
-   Temporal Client
-   Worker
-   Workflow
-   Activity
-   starting a Workflow
-   retrieving Workflow result
-   task queues

## Build

Create:

``` text
temporal-learning/
├── pyproject.toml
├── src/
│   ├── workflows/
│   ├── activities/
│   ├── workers/
│   └── clients/
├── tests/
└── README.md
```

Build:

``` text
Client -> Workflow -> Activity -> Result
```

## Exercises

-   [ ] Hello World Workflow
-   [ ] Workflow accepting input
-   [ ] Workflow returning structured output
-   [ ] Workflow invoking an Activity
-   [ ] Worker running the Workflow
-   [ ] Client starting the Workflow
-   [ ] Query the execution using the Temporal UI
-   [ ] Stop the Worker while the Workflow is running
-   [ ] Restart the Worker
-   [ ] Observe what happens

## Mastery gate

Explain the lifecycle:

``` text
Workflow requested
    ↓
Temporal records execution
    ↓
Workflow Task scheduled
    ↓
Worker receives task
    ↓
Workflow schedules Activity
    ↓
Activity Task scheduled
    ↓
Worker executes Activity
    ↓
Activity result recorded
    ↓
Workflow continues
```

------------------------------------------------------------------------

# 4. Phase 3 --- Workflow Fundamentals

## Goal

Become excellent at writing correct Python Workflows.

## Topics

-   Workflow definitions
-   Workflow inputs
-   Workflow outputs
-   Workflow state
-   Activity invocation
-   timers
-   conditions
-   loops
-   Workflow execution lifecycle
-   Workflow IDs
-   Run IDs
-   Workflow history

## Critical concept: Determinism

Study deeply:

-   deterministic Workflow code
-   replay
-   Workflow history
-   why arbitrary external calls are unsafe
-   why random values can be problematic
-   why direct system time can be problematic
-   why filesystem/database/network access does not belong directly in
    Workflow code
-   how Temporal APIs provide deterministic alternatives

## Exercise

Write a Workflow that:

1.  receives an order,
2.  validates it,
3.  waits,
4.  calls an Activity,
5.  waits for another condition,
6.  completes.

Then identify which lines are Workflow-safe and which must become
Activities.

## Deliberate failure lab

Put these into a Workflow one at a time:

-   direct HTTP call
-   direct database query
-   random number generation
-   uncontrolled current time
-   filesystem operation

Ask the agent to explain the determinism implications and replace each
with the appropriate Temporal pattern.

## Mastery gate

Given arbitrary Python code, I can classify each operation as:

-   Workflow-safe
-   Activity-only
-   potentially nondeterministic
-   requiring a Temporal-specific API

------------------------------------------------------------------------

# 5. Phase 4 --- Activities

## Goal

Master reliable execution of external side effects.

## Topics

-   Activity definitions
-   Activity inputs/outputs
-   synchronous versus asynchronous Activities
-   Activity retries
-   retry policies
-   timeouts
-   Schedule-To-Start
-   Start-To-Close
-   Schedule-To-Close
-   heartbeat timeouts
-   heartbeats
-   cancellation
-   idempotency
-   external side effects

## Build

Create Activities for:

``` text
create_customer()
charge_payment()
reserve_inventory()
send_email()
write_database()
call_external_api()
```

## Failure labs

For each Activity:

-   fail once
-   fail three times
-   fail permanently
-   sleep longer than its timeout
-   simulate Worker crash
-   simulate external API timeout
-   simulate duplicate execution

Observe:

-   retries
-   Workflow history
-   Activity state
-   final Workflow behavior

## Deep-dive question

Why does an Activity generally need to be designed as if it can execute
more than once?

## Mastery gate

I can design an Activity with:

-   appropriate timeout
-   retry policy
-   idempotency strategy
-   failure classification
-   heartbeat strategy if long-running
-   cancellation behavior

------------------------------------------------------------------------

# 6. Phase 5 --- Temporal Failure Semantics

## Goal

Understand failures at an expert level.

## Topics

-   Workflow failure
-   Activity failure
-   retryable versus non-retryable failures
-   exceptions
-   timeouts
-   cancellation
-   termination
-   retry exhaustion
-   child Workflow failure
-   Worker crash
-   Temporal Service failure concepts
-   external dependency failure

## Build a failure matrix

  Failure                  Expected behavior   Retry?     Where handled?
  ------------------------ ------------------- ---------- -------------------------
  HTTP 500                 Retry               Usually    Activity
  HTTP 400                 Usually no          No         Activity
  Database deadlock        Retry               Usually    Activity
  Invalid business input   No                  No         Workflow/business logic
  Worker crash             Resume              Temporal   Service/history
  Long-running Activity    Heartbeat           Depends    Activity
  Workflow cancellation    Cancel              No         Workflow

## Mastery gate

Given a production incident, I can identify:

1.  what failed,
2.  where it failed,
3.  whether Temporal retries it,
4.  whether the external side effect may have happened,
5.  whether the operation must be idempotent,
6.  what the correct recovery strategy is.

------------------------------------------------------------------------

# 7. Phase 6 --- Signals, Queries, Updates, and External Interaction

## Goal

Build interactive Workflows.

## Topics

-   Signals
-   Queries
-   Updates
-   external Workflow interaction
-   waiting for external events
-   Workflow state machines
-   asynchronous business processes

## Build

Create an order Workflow:

``` text
ORDER_CREATED
      |
      v
WAITING_FOR_PAYMENT
      |
      v
PAYMENT_CONFIRMED
      |
      v
FULFILLING
      |
      v
COMPLETED
```

Support:

-   payment confirmation Signal
-   cancellation Signal
-   status Query
-   customer-address Update
-   timeout for payment

## Mastery gate

Explain the difference between:

-   starting a Workflow
-   signaling a Workflow
-   querying a Workflow
-   updating a Workflow
-   calling an Activity

and when each should be used.

------------------------------------------------------------------------

# 8. Phase 7 --- Timers, Waiting, Cancellation, and Long-Running Workflows

## Goal

Become comfortable with Workflows that live for hours, days, months, or
longer.

## Topics

-   Workflow timers
-   durable waiting
-   cancellation scopes
-   cancellation propagation
-   graceful cleanup
-   long-running state machines
-   continue-as-new
-   Workflow history growth
-   long-running Workflow design

## Build

Create a subscription lifecycle:

``` text
SUBSCRIBED
   |
   +--> wait 30 days
   |
   v
RENEWAL_DUE
   |
   +--> wait for payment
   |
   +--> cancel
   |
   +--> renew
```

## Advanced lab

Run the Workflow for simulated months while keeping the test fast.

Investigate:

-   history growth
-   Continue-As-New
-   state preservation
-   cancellation
-   timers

## Mastery gate

I can explain when a Workflow should use Continue-As-New and how
long-running execution affects Workflow history.

------------------------------------------------------------------------

# 9. Phase 8 --- Child Workflows and Composition

## Goal

Build large systems from smaller durable components.

## Topics

-   Child Workflows
-   parent/child relationships
-   child failure
-   child cancellation
-   parallel child Workflows
-   sequential versus concurrent orchestration
-   fan-out/fan-in
-   Workflow composition

## Build

Create:

``` text
OrderWorkflow
   |
   +-- PaymentWorkflow
   |
   +-- InventoryWorkflow
   |
   +-- FulfillmentWorkflow
   |
   +-- NotificationWorkflow
```

Then build a parallel version where independent operations execute
concurrently.

## Mastery gate

Given a large Workflow, decide:

-   what belongs in the parent,
-   what belongs in an Activity,
-   what deserves a Child Workflow,
-   what should execute in parallel.

------------------------------------------------------------------------

# 10. Phase 9 --- Task Queues, Workers, Scaling, and Routing

## Goal

Understand how Temporal applications scale.

## Topics

-   Task Queues
-   Worker processes
-   Worker concurrency
-   multiple Workers
-   Worker deployment
-   routing
-   workload isolation
-   specialized Workers
-   scaling
-   task distribution
-   worker capacity

## Build

Create separate task queues:

``` text
payments
inventory
notifications
data-processing
```

Run specialized Workers.

Example:

``` text
Order Workflow
      |
      +---- payments queue ----> Payment Workers
      |
      +---- inventory queue ---> Inventory Workers
      |
      +---- notification queue -> Notification Workers
```

## Exercise

Scale each Worker independently.

Ask:

-   What happens if payment Workers are overloaded?
-   Can notification processing affect payment processing?
-   How should task queues be separated?
-   How should Worker capacity be measured?

## Mastery gate

Design a Worker topology for a production application with:

-   10,000 Workflow executions/hour
-   slow external APIs
-   CPU-heavy processing
-   independent scaling requirements.

------------------------------------------------------------------------

# 11. Phase 10 --- Testing Temporal Applications

## Goal

Write tests that provide confidence in durable Workflow behavior.

## Topics

-   Workflow unit tests
-   Activity tests
-   integration tests
-   time skipping
-   replay testing
-   failure testing
-   mocking Activities
-   deterministic testing
-   test environments

## Test categories

### Unit tests

-   Workflow logic
-   Activity logic

### Workflow tests

-   success path
-   Activity failure
-   retries
-   timeout
-   cancellation
-   Signal
-   Query
-   Update
-   timer
-   child Workflow

### Integration tests

-   real Temporal Service/development environment
-   real external dependencies where appropriate

## Required test suite

Build at least:

-   [ ] 20 Workflow tests
-   [ ] 15 Activity tests
-   [ ] 5 failure/retry tests
-   [ ] 5 cancellation tests
-   [ ] 5 timer tests
-   [ ] 5 Signal/Update tests
-   [ ] 5 integration tests

## Mastery gate

I can look at a Temporal feature and explain how I would test:

1.  normal behavior,
2.  failure behavior,
3.  retry behavior,
4.  cancellation,
5.  Worker crash/replay implications.

------------------------------------------------------------------------

# 12. Phase 11 --- Workflow Versioning and Safe Evolution

## Goal

Learn how production systems evolve without breaking existing
executions.

## Topics

-   Workflow replay
-   incompatible code changes
-   Workflow history compatibility
-   versioning strategies
-   Worker deployment strategies
-   rolling deployments
-   old versus new Workflow executions
-   safe schema evolution
-   payload evolution

## Critical exercise

Start a long-running Workflow using version 1.

Then change:

``` python
step_a()
step_b()
```

to:

``` python
step_a()
step_c()
step_b()
```

Investigate replay behavior.

Then learn and implement the correct Temporal-supported
versioning/evolution strategy.

## Mastery gate

I can answer:

> "Can I deploy this Workflow code change while existing Workflows are
> still running?"

and justify the answer technically.

------------------------------------------------------------------------

# 13. Phase 12 --- Data Conversion, Serialization, and Payload Design

## Goal

Understand how data crosses the Temporal boundary.

## Topics

-   Workflow inputs
-   Activity inputs
-   Workflow results
-   serialization
-   payloads
-   type hints
-   structured data
-   large payload concerns
-   external storage references
-   schema evolution
-   sensitive data considerations

## Exercise

Design payloads for:

``` text
Customer
Order
Payment
DataPipelineRun
JobStatus
```

Compare:

``` text
Pass entire dataset
        vs
Pass object-storage URI
        vs
Pass database reference
```

## Mastery gate

I can decide when data should be passed through Temporal and when
Temporal should carry a reference to external data.

------------------------------------------------------------------------

# 14. Phase 13 --- Production Observability

## Goal

Operate Temporal applications confidently.

## Topics

-   Temporal UI
-   Workflow visibility
-   Workflow history
-   logs
-   metrics
-   tracing concepts
-   Worker health
-   Activity latency
-   retry rates
-   task queue backlog
-   failure rates
-   operational dashboards
-   alerts

## Build a production dashboard

Track at minimum:

-   Workflow success rate
-   Workflow failure rate
-   Activity failure rate
-   Activity retry rate
-   Activity latency
-   Workflow duration
-   Task Queue backlog
-   Worker availability
-   timeout count
-   cancellation count

## Incident exercises

Simulate:

-   Worker outage
-   Activity dependency outage
-   task queue backlog
-   bad deployment
-   runaway retries
-   stuck Workflow
-   non-deterministic Workflow change

## Mastery gate

Given only metrics and Workflow history, I can investigate a failed
production execution and produce a root-cause hypothesis.

------------------------------------------------------------------------

# 15. Phase 14 --- Security and Production Architecture

## Goal

Design secure Temporal systems.

## Topics

-   authentication
-   authorization
-   namespaces
-   TLS
-   credentials
-   secrets
-   network security
-   least privilege
-   sensitive payloads
-   encryption considerations
-   production isolation
-   multi-environment architecture

## Design environments

``` text
Development
     |
     v
Test
     |
     v
Staging
     |
     v
Production
```

Understand what should and should not be shared between environments.

## Mastery gate

Produce a security architecture for a production Temporal deployment and
explain every trust boundary.

------------------------------------------------------------------------

# 16. Phase 15 --- Temporal Cloud and Self-Hosted Temporal

## Goal

Understand both deployment models.

## Topics

-   Temporal Cloud
-   self-hosted Temporal
-   namespaces
-   workers versus Temporal Service
-   connectivity
-   TLS
-   authentication
-   deployment architecture
-   operational responsibility
-   upgrades
-   backups/recovery concepts
-   scaling

## Exercise

Create two architecture diagrams:

### Managed

``` text
Application
   |
Workers
   |
Temporal Cloud
```

### Self-hosted

``` text
Application
   |
Workers
   |
Load Balancer
   |
Temporal Service
   |
Persistence / infrastructure
```

Document operational responsibilities for each.

## Mastery gate

Given a company scenario, recommend Temporal Cloud or self-hosted and
explain the trade-offs.

------------------------------------------------------------------------

# 17. Phase 16 --- AWS + Temporal

## Goal

Apply Temporal to real cloud/data-engineering problems.

This phase is especially important for production architecture.

## Integrations to practice

-   AWS Lambda
-   ECS/Fargate
-   EKS
-   S3
-   RDS/PostgreSQL
-   DynamoDB
-   SQS/SNS
-   EventBridge
-   Secrets Manager
-   CloudWatch
-   API Gateway

## Build Project

### Durable Data Pipeline Orchestrator

``` text
API / Event
    |
    v
Temporal Workflow
    |
    +--> Validate input
    |
    +--> Extract data
    |
    +--> Store raw data in S3
    |
    +--> Start processing
    |
    +--> Run validation
    |
    +--> Load database
    |
    +--> Publish completion event
    |
    +--> Notify user
```

Requirements:

-   retries
-   idempotency
-   timeout policies
-   failure recovery
-   cancellation
-   observability
-   auditability

------------------------------------------------------------------------

# 18. Phase 17 --- Temporal for Data Engineering

## Goal

Use Temporal where it provides real value in data platforms.

## Compare

Understand when to use:

-   Temporal
-   Airflow
-   AWS Step Functions
-   EventBridge
-   SQS
-   Lambda
-   Glue Workflows
-   Dagster
-   traditional application orchestration

Do not assume Temporal replaces every orchestrator.

## Build

Create:

``` text
DataPipelineWorkflow
   |
   +-- ExtractActivity
   |
   +-- ValidateActivity
   |
   +-- TransformActivity
   |
   +-- LoadActivity
   |
   +-- QualityCheckActivity
   |
   +-- PublishActivity
```

Add:

-   retries
-   dependency handling
-   partial failure recovery
-   manual approval Signal
-   status Query
-   cancellation
-   compensation

------------------------------------------------------------------------

# 19. Phase 18 --- Advanced Patterns

## Goal

Move from "Temporal developer" to "Temporal architect."

Study and implement:

-   Saga/compensation patterns
-   idempotency
-   distributed transactions
-   fan-out/fan-in
-   rate limiting
-   batching
-   concurrency control
-   external event coordination
-   human-in-the-loop workflows
-   long-running business processes
-   scheduled workflows
-   recurring workflows
-   workflow search/visibility patterns
-   multi-tenant architecture
-   worker isolation
-   backpressure
-   graceful shutdown
-   failure recovery
-   workflow replay
-   deterministic state machines

## Advanced project

Build a complete:

### Order Fulfillment Platform

Requirements:

-   order creation
-   payment
-   fraud check
-   inventory
-   shipping
-   customer notification
-   cancellation
-   compensation
-   human approval
-   retries
-   timeouts
-   external Signals
-   Queries
-   Updates
-   Child Workflows
-   task queues
-   multiple Worker types
-   observability
-   tests
-   versioned deployment

------------------------------------------------------------------------

# 20. Phase 19 --- Source-Code and SDK Internals

## Goal

Understand enough of the Python SDK to debug difficult issues.

Study:

-   Temporal Python SDK architecture
-   Client internals
-   Worker internals
-   Workflow execution model
-   Activity execution model
-   task polling
-   payload conversion
-   replay
-   sandboxing/determinism concepts
-   SDK/runtime interaction
-   error propagation

## Exercise

Pick a simple Workflow and trace:

``` text
Client
  ↓
Temporal Service
  ↓
Workflow Task
  ↓
Python Worker
  ↓
Workflow code
  ↓
Activity Task
  ↓
Activity Worker
  ↓
Temporal Service
  ↓
Workflow continuation
```

Write a technical explanation of every transition.

------------------------------------------------------------------------

# 21. Capstone Project

## Build: Production-Grade Data Processing Platform

Build a complete Temporal-based system.

### Scenario

A user submits a data-processing request.

``` text
User
 |
 v
API
 |
 v
Temporal Workflow
 |
 +--> Validate request
 |
 +--> Create processing record
 |
 +--> Download source data
 |
 +--> Process data
 |
 +--> Run data quality checks
 |
 +--> Store results
 |
 +--> Update metadata
 |
 +--> Publish event
 |
 +--> Notify user
```

### Required features

-   [ ] Python SDK
-   [ ] Workflow
-   [ ] Activities
-   [ ] Worker
-   [ ] Client
-   [ ] Task Queues
-   [ ] retries
-   [ ] timeouts
-   [ ] heartbeats
-   [ ] cancellation
-   [ ] Signals
-   [ ] Queries
-   [ ] Updates
-   [ ] Child Workflows
-   [ ] timers
-   [ ] Continue-As-New where appropriate
-   [ ] idempotency
-   [ ] failure handling
-   [ ] testing
-   [ ] observability
-   [ ] versioning
-   [ ] deployment
-   [ ] security
-   [ ] documentation

### Production constraints

The agent should deliberately inject:

-   Worker crashes
-   network failures
-   API failures
-   duplicate Activity execution
-   database failures
-   timeout conditions
-   cancellation
-   deployment/version changes

The system must recover correctly.

------------------------------------------------------------------------

# 22. Mastery Levels

## Level 1 --- Beginner

I can:

-   run Temporal locally
-   create a Workflow
-   create an Activity
-   create a Worker
-   start a Workflow
-   retrieve a result

## Level 2 --- Intermediate

I can:

-   use retries
-   configure timeouts
-   use timers
-   use Signals
-   use Queries
-   handle failures
-   test Workflows
-   explain determinism

## Level 3 --- Advanced

I can:

-   design long-running Workflows
-   use Child Workflows
-   use Continue-As-New
-   design task queues
-   scale Workers
-   handle cancellation
-   design idempotent Activities
-   evolve Workflow code safely
-   investigate Workflow history

## Level 4 --- Production Engineer

I can:

-   deploy Temporal applications
-   build observability
-   operate Workers
-   diagnose production incidents
-   design security
-   manage deployments
-   perform failure testing
-   integrate Temporal with cloud services

## Level 5 --- Temporal Architect / Pro

I can:

-   select Temporal versus alternative orchestration technologies
-   design large-scale Workflow systems
-   reason about failure semantics
-   design multi-tenant architectures
-   design Worker topology
-   solve long-running business-process problems
-   review Temporal architecture
-   identify nondeterminism risks
-   design versioning strategies
-   perform capacity planning
-   create operational runbooks
-   mentor other engineers

------------------------------------------------------------------------

# 23. Agent Teaching Protocol

When this `plan.md` is given to an AI agent, the agent MUST follow this
protocol.

## Rule 1 --- Do not dump the entire course

Teach one concept at a time.

Use:

``` text
Explain
→ Example
→ Ask questions
→ Coding exercise
→ Review
→ Failure exercise
→ Mastery test
→ Next concept
```

## Rule 2 --- Make me code

For every important concept:

1.  explain it briefly,
2.  show the smallest useful example,
3.  give me a coding task,
4.  review my solution,
5.  introduce one failure scenario.

## Rule 3 --- Use Socratic questioning

Before explaining difficult topics, ask questions such as:

-   What happens if the Worker crashes?
-   What happens if the Activity executes twice?
-   Where is state stored?
-   Can this code replay?
-   Is this operation deterministic?
-   What happens if the API returns 500?
-   What happens if the response is lost after the API committed the
    change?

## Rule 4 --- Do not let me memorize

If I give a definition, ask me to explain it with a real example.

If I provide code, ask me what happens during failure.

If I say "Temporal retries it," ask:

> "What exactly is being retried, under what policy, and what happens to
> the external side effect?"

## Rule 5 --- Prefer current official documentation

Use the official Temporal documentation as the primary source.

When an API or behavior may have changed, verify the current
documentation before teaching it.

Do not confidently teach an old API from memory.

## Rule 6 --- Explain Python-specific behavior

Whenever Temporal interacts with:

-   `asyncio`
-   decorators
-   exceptions
-   serialization
-   concurrency
-   type hints
-   context managers

explain the Python implications as well.

## Rule 7 --- Every phase ends with a gate

Do not move to the next phase until I can pass:

-   conceptual questions,
-   coding exercise,
-   failure scenario,
-   architecture question.

If I fail, reteach the weak area using a different example.

------------------------------------------------------------------------

# 24. Agent Response Format

For each lesson, use:

## Lesson

### 1. Objective

What I will learn.

### 2. Mental model

Explain the concept in simple language.

### 3. Temporal model

Explain how Temporal actually executes it.

### 4. Python implementation

Show a minimal example.

### 5. Exercise

Give me a task to implement.

### 6. Failure exercise

Give me one failure scenario to reason about.

### 7. Review

Review my answer/code.

### 8. Mastery questions

Ask 3--7 questions.

### 9. Gate

Tell me whether I passed.

### 10. Next step

Only continue when I pass.

------------------------------------------------------------------------

# 25. Required Question Bank

The agent should repeatedly challenge me with questions like:

### Fundamentals

1.  What is durable execution?
2.  What is a Workflow?
3.  What is an Activity?
4.  What is a Worker?
5.  What is a Task Queue?
6.  What is Workflow history?

### Determinism

7.  Why must Workflow code be deterministic?
8.  What happens during replay?
9.  Why should external API calls normally be Activities?
10. What kinds of Python operations can cause nondeterminism?

### Reliability

11. Why are Activity retries different from Workflow retries?
12. Why does idempotency matter?
13. When should an Activity heartbeat?
14. When should an Activity not retry?
15. How should a timeout be selected?

### Interaction

16. Signal vs Query vs Update?
17. When should a Workflow wait for an external event?
18. How does cancellation propagate?

### Architecture

19. When should I use a Child Workflow?
20. When should I use an Activity?
21. How should Task Queues be separated?
22. How do Workers scale?
23. How should CPU-heavy work be isolated?

### Production

24. How do I deploy a new Workflow version safely?
25. How do I investigate a stuck Workflow?
26. How do I identify a retry storm?
27. How do I design observability?
28. How do I protect sensitive Workflow data?

### Architecture comparison

29. Temporal vs Airflow?
30. Temporal vs Step Functions?
31. Temporal vs SQS?
32. Temporal vs EventBridge?
33. Temporal vs a database-backed state machine?

------------------------------------------------------------------------

# 26. Weekly Learning Schedule

A practical 12-week schedule:

  -----------------------------------------------------------------------
  Week                    Focus                   Deliverable
  ----------------------- ----------------------- -----------------------
  1                       Distributed systems +   Architecture notes
                          Temporal fundamentals   

  2                       Python SDK + Hello      First Workflow
                          World                   

  3                       Workflows + determinism Deterministic Workflow
                                                  lab

  4                       Activities + retries +  Reliable API
                          timeouts                integration

  5                       Failure handling +      Failure lab
                          idempotency             

  6                       Signals + Queries +     Interactive order
                          Updates                 Workflow

  7                       Timers + cancellation + Subscription Workflow
                          long-running Workflows  

  8                       Child Workflows + Task  Multi-Worker system
                          Queues                  

  9                       Testing + versioning    Production test suite

  10                      Observability +         Operational
                          security                dashboard/design

  11                      AWS + data engineering  Durable data pipeline

  12                      Capstone + architecture Production-grade system
                          review                  
  -----------------------------------------------------------------------

Do not treat the weeks as deadlines. If a concept is not understood,
stay on the phase until the mastery gate is passed.

------------------------------------------------------------------------

# 27. Daily Learning Routine

Use approximately 60--90 minutes per session.

### 10 minutes

Review yesterday's concepts.

### 20 minutes

Learn one new concept.

### 30--45 minutes

Write code.

### 10 minutes

Break the code deliberately.

### 10 minutes

Answer mastery questions.

### Final question

Always answer:

> "What happens if the Worker crashes at this exact line?"

------------------------------------------------------------------------

# 28. Final Proficiency Assessment

The agent should give a final assessment only after the capstone.

## Part A --- Concepts

25 questions.

## Part B --- Debugging

Give me 5 broken Temporal applications.

I must diagnose them.

## Part C --- Coding

Build a Workflow from a business requirement without being given the
architecture.

## Part D --- Failure Design

Give me 10 failure scenarios.

I must explain the recovery behavior.

## Part E --- Architecture

Design a Temporal platform for:

``` text
1 million Workflow executions/day
10,000 concurrent Workflows
multiple Worker types
external APIs
database dependencies
human approval
long-running processes
multi-environment deployment
production observability
```

## Part F --- Code Review

Give me intentionally incorrect Temporal code containing:

-   nondeterminism
-   incorrect retry policy
-   missing timeout
-   non-idempotent Activity
-   inappropriate Workflow logic
-   poor task queue design
-   unsafe version change

I must identify every problem.

------------------------------------------------------------------------

# 29. Definition of "Temporal Pro"

I should not consider myself proficient merely because I can write:

``` python
@workflow.defn
class MyWorkflow:
    ...
```

I am "Temporal Pro" when I can look at a business process and determine:

1.  What should be a Workflow?
2.  What should be an Activity?
3.  What should be a Child Workflow?
4.  What state belongs in Workflow state?
5.  What data belongs outside Temporal?
6.  What can fail?
7.  What can execute twice?
8.  What should retry?
9.  What should timeout?
10. What should heartbeat?
11. How should cancellation work?
12. How should the system scale?
13. How should Workers be separated?
14. How will the Workflow evolve?
15. How will it be tested?
16. How will it be monitored?
17. How will it be secured?
18. How will it be deployed?
19. How will production incidents be diagnosed?
20. Why is Temporal the right technology for this problem?

------------------------------------------------------------------------

# 30. First Instruction to the Learning Agent

Start by asking me:

> "What is your current Python level, and have you worked with
> distributed systems, Airflow, AWS Step Functions, or another workflow
> orchestrator?"

Then assess my prerequisite knowledge with 10 questions.

Do **not** start teaching Temporal immediately.

Based on my answers:

-   identify prerequisite gaps,
-   create a personalized starting point,
-   start Phase 1,
-   maintain a progress record,
-   track failed mastery gates,
-   revisit weak concepts,
-   and progressively increase difficulty.

At every stage, prioritize **hands-on Python implementation and failure
reasoning** over passive reading.

------------------------------------------------------------------------

## Progress Tracker

### Prerequisites

-   [ ] Python async/await
-   [ ] Python testing
-   [ ] Distributed-systems basics

### Temporal Core

-   [ ] Temporal architecture
-   [ ] Client
-   [ ] Worker
-   [ ] Workflow
-   [ ] Activity
-   [ ] Task Queue
-   [ ] Workflow history
-   [ ] Determinism

### Reliability

-   [ ] Retries
-   [ ] Timeouts
-   [ ] Heartbeats
-   [ ] Cancellation
-   [ ] Idempotency
-   [ ] Failure handling

### Workflow Features

-   [ ] Signals
-   [ ] Queries
-   [ ] Updates
-   [ ] Timers
-   [ ] Child Workflows
-   [ ] Continue-As-New

### Production

-   [ ] Testing
-   [ ] Versioning
-   [ ] Observability
-   [ ] Security
-   [ ] Scaling
-   [ ] Deployment

### Architecture

-   [ ] Temporal vs Airflow
-   [ ] Temporal vs Step Functions
-   [ ] Temporal vs queues
-   [ ] Data engineering patterns
-   [ ] AWS architecture
-   [ ] Multi-tenant architecture

### Final

-   [ ] Capstone completed
-   [ ] Failure assessment passed
-   [ ] Architecture assessment passed
-   [ ] Code review assessment passed
-   [ ] Temporal Pro assessment passed

------------------------------------------------------------------------

# 31. Reference Strategy

Use the official Temporal documentation as the source of truth:

-   Python SDK documentation
-   Temporal Developer Guide
-   Workflow documentation
-   Activity documentation
-   Worker documentation
-   Testing documentation
-   Production/deployment documentation
-   Temporal Cloud documentation

When this learning plan conflicts with the current SDK documentation,
**the current official documentation wins**.
