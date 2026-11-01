# Phase 16: AWS + Temporal

## 1. Objective

Connect Temporal to real cloud services, and choose the right idempotency approach for each.

## 2. Mental model

Temporal orchestrates, and AWS does the work. Each AWS service plays one of a few roles:

| Role | Services |
|---|---|
| Starts Workflows | API Gateway, Lambda, EventBridge, SQS consumer |
| Runs Workers | ECS/Fargate, EKS, EC2 |
| Called by Activities | S3, RDS, DynamoDB, SNS, SQS, Glue, Lambda |
| Supports everything | Secrets Manager, CloudWatch |

## 3. Temporal model

### Reference architecture

```text
Client --> API Gateway --> Lambda --> start_workflow
EventBridge rule ------------------^
SQS consumer ---- signal / start --^
                                   |
                                   v
                    Temporal Cloud or self-hosted
                                   ^
                                   | Workers poll, outbound only
         +-------------------------+----------------------+
         |                         |                      |
   ECS: orchestration        ECS: IO Activities      EKS or ECS: data
         |                         |                      |
         +-> CloudWatch            +-> S3, DynamoDB       +-> S3
                                   +-> SNS, SQS
                                   +-> Secrets Manager
```

### Why ECS or EKS for Workers, not Lambda?

A classic Worker is a long-running process that polls constantly, which suits containers. Lambda suits the *front door*: a short function that calls `start_workflow`.

The installed SDK does ship `temporalio.contrib.aws.lambda_worker`, so Worker-on-Lambda now exists. I haven't used it here, so read its README and the docs before relying on it.

### Idempotency, per service

This is the most valuable table in this phase:

| Service | Approach | Why it works |
|---|---|---|
| S3 | Key derived from the run ID | A retry overwrites identical bytes |
| DynamoDB | `attribute_not_exists` condition | The second write is a no-op |
| RDS/PostgreSQL | Unique key, `ON CONFLICT DO NOTHING` | The database rejects a repeat |
| SNS, SQS | Marker first, consumers deduplicate | Delivery is at-least-once, so it can't be perfect |
| Lambda | Pass a request ID, and deduplicate inside | Same reason |

The SNS row isn't airtight, and that's honest: if the Worker dies between writing the marker and publishing, no message goes out. Reverse the order and you might publish twice. For a truly critical event, look at the outbox pattern, and always make consumers tolerate duplicates.

### Classifying AWS errors

```python
NON_RETRYABLE_CODES = {"AccessDenied", "NoSuchBucket", "NoSuchKey", ...}
```

`ThrottlingException` will pass with time, so retry it. `AccessDenied` won't, so stop.

### Secrets

Read them inside the Activity, at the moment of use. Anything a Workflow touches is written to history.

## 4. Python implementation

Read [activities/aws_pipeline.py](../../temporal-learning/src/activities/aws_pipeline.py) and [workflows/aws_pipeline.py](../../temporal-learning/src/workflows/aws_pipeline.py).

```powershell
pytest tests/test_phase16_aws.py -v
```

**These run real boto3 calls against moto, an in-process AWS fake. No AWS account was used, and nothing here has touched real AWS.** Real IAM, networking, and service limits weren't tested, and moto doesn't copy AWS perfectly.

| Plan requirement | Where |
|---|---|
| Retries | `AWS_CALL` policy |
| Idempotency | Deterministic keys and conditional writes |
| Timeout policies | `start_to_close_timeout`, and a heartbeat on `load_table` |
| Failure recovery | `classify`, and Temporal's retries |
| Cancellation | `delete_outputs` cleanup, tested mid-Activity |
| Observability | `current_stage` Query |
| Auditability | The Event History itself |

**Python detail:** boto3 is blocking, so these are sync Activities on a thread pool. `boto3.client` calls aren't all safe to share across threads casually, so a client is created per call here. In production, reuse clients carefully.

## 5. Exercise

Add an RDS step: load rows into PostgreSQL with a unique key so that a retry is harmless. Use Docker for a local PostgreSQL if you can. Then write the IAM policy for the IO Workers, granting only what they need.

## 6. Failure exercise

`load_table` has written 40,000 of 100,000 rows when the Worker dies. Walk through the retry: how does it avoid duplicates, and how much work repeats? How would heartbeat details reduce that? What's the risk if the source file changed between attempts?

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. Why should Workers usually run on ECS or EKS?
2. How do you make an S3 write idempotent? A DynamoDB write?
3. Why can't you get exactly-once delivery through SNS?
4. Where should a database password be read, and why?
5. How does an EventBridge event become a Workflow?

## 9. Gate

Type `/gate`.

## 10. Next step

**Phase 17: Temporal for Data Engineering.**
