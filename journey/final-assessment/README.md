# Final Proficiency Assessment

Your plan says this comes **only after the capstone**. Type `/gate` and ask for the final assessment when you're ready. It can't be passed by reading, because your mentor will run it live, and follow up on your answers.

## Part A: Concepts (25 questions)

Answer aloud or in writing, with real examples.

1. What is durable execution?
2. What's the difference between a Workflow and a Workflow Execution?
3. Where does Workflow state live while a run is asleep?
4. What's a Workflow Task?
5. How does the Service detect a dead Worker?
6. Why must Workflow code be deterministic?
7. What happens during replay, line by line?
8. Which Python operations cause nondeterminism?
9. Is a Workflow that passes the sandbox safe? Why not?
10. Why can't Temporal give exactly-once side effects?
11. Why are Activity retries different from Workflow retries?
12. How do you choose a Start-To-Close timeout?
13. When should an Activity heartbeat?
14. When should an Activity not retry?
15. Signal, Query, or Update: which, and when?
16. How does cancellation reach an Activity? A waiting Workflow?
17. When is Continue-As-New needed, and what survives it?
18. Child Workflow or Activity?
19. How should Task Queues be separated?
20. How do you size Worker capacity?
21. How do you deploy a Workflow change safely?
22. What does a Payload Codec not protect?
23. When should Temporal carry a reference and not the data?
24. How do you identify a retry storm?
25. When is Temporal the wrong choice?

## Part B: Debugging

[broken_apps.py](broken_apps.py) holds five broken Temporal applications. **Every bug is one that really happened while this course was being built.** For each, find the bug, explain the symptom in production, and fix it.

## Part C: Coding

Your mentor gives you a business requirement with no architecture. You design and build it, with tests.

## Part D: Failure design

Explain the recovery behaviour, and what you'd need to have built beforehand:

1. The Worker dies between an Activity finishing and reporting.
2. A dependency is down for three days.
3. A deploy adds an Activity in the middle of a Workflow with 10,000 open runs.
4. A Signal arrives while the Workflow is running an Activity.
5. A cancel arrives during compensation.
6. The final retry of a charge times out.
7. The Temporal Service is unreachable for an hour.
8. A payload exceeds the size limit in the middle of a run.
9. The encryption key is lost.
10. Two clients start the same order at the same moment.

## Part E: Architecture

Design a Temporal platform for:

```text
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

Include queues, Worker sizing with your arithmetic, namespaces, versioning, data placement, security, dashboards, and failure handling.

## Part F: Code review

[review_this.py](review_this.py) contains intentionally incorrect code. Find every problem: nondeterminism, incorrect retry policy, missing timeout, non-idempotent Activity, inappropriate Workflow logic, poor task queue design, and an unsafe version change.
