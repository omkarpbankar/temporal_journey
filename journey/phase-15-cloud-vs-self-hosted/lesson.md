# Phase 15: Temporal Cloud and Self-Hosted Temporal

## 1. Objective

Recommend Cloud or self-hosted for a given company, and explain the trade-offs.

## 2. Mental model

It's like choosing between a managed database such as RDS and running PostgreSQL yourself. The engine is the same. The question is who gets paged at 3 a.m.

**Either way, you run your own Workers.** Your code and data stay in your network. Only the Service moves.

## 3. Temporal model

### Managed

```text
Application
   |
Workers (your infrastructure)
   |  TLS + API key or mTLS
Temporal Cloud
```

### Self-hosted

```text
Application
   |
Workers (your infrastructure)
   |
Load Balancer
   |
Temporal Service (Frontend, History, Matching, Worker)
   |
Persistence (PostgreSQL, MySQL, or Cassandra) and Visibility (e.g. Elasticsearch)
```

### Who does what

| Responsibility | Cloud | Self-hosted |
|---|---|---|
| Your Workers and code | You | You |
| Service availability | Temporal | **You** |
| Database, backups, and recovery | Temporal | **You** |
| Upgrades and schema migrations | Temporal | **You** |
| Scaling the Service, shard planning | Temporal | **You** |
| TLS and authorization setup | Shared | **You** |
| Monitoring the Service | Temporal | **You** |
| Monitoring Workers | You | You |
| Payload encryption | You | You |
| Billing | Usage-based | Your infrastructure and staff |

Self-hosting notes from the docs:

- With the `temporalio/server` image, you manage schema updates yourself, using `temporalio/admin-tools`.
- Helm is the standard route for Kubernetes. Temporal Server 1.30+ images need Helm chart 0.73.1 or later.
- Secure the Service "similarly to a database," and run it only where trusted internal networks can reach it.
- The dev server is for development and testing, never production.

### How to decide

| Lean towards Cloud | Lean towards self-hosted |
|---|---|
| Small platform team | A strong platform team that already runs databases at scale |
| You want to start quickly | Rules that require running everything yourself |
| High availability matters and you lack the staff for it | Very high steady volume, where the sums favour it |
| Your focus is the product | No outside connectivity allowed |

Check Temporal's current pricing and compliance pages for a real decision. Neither changes quickly, but both matter too much to assume.

Source: [Self-hosted deployment](https://docs.temporal.io/self-hosted-guide/deployment), [Client connection](https://docs.temporal.io/develop/python/temporal-client)

## 4. Python implementation

**Your code doesn't change between them.** [shared/connection.py](../../temporal-learning/src/shared/connection.py) reads its settings from the environment:

```powershell
# Local: set nothing.

# Temporal Cloud
$env:TEMPORAL_ADDRESS = "your-namespace.a1b2c.tmprl.cloud:7233"
$env:TEMPORAL_NAMESPACE = "your-namespace.a1b2c"
$env:TEMPORAL_API_KEY = "<your key>"

# Self-hosted
$env:TEMPORAL_ADDRESS = "temporal.internal.example:7233"
```

Or use a TOML profile. Environment variables win over the file.

```powershell
pytest tests/test_phase15_connection.py -v
```

These tests prove the settings are read correctly, and that `connect()` reaches a real local server.

### What wasn't run

**I didn't connect to Temporal Cloud**, because that needs an account. **I didn't run a self-hosted cluster either.** I tried to clone Temporal's Docker sample and start it, and this environment's permission rules blocked running external code, so I stopped there. These steps come from the docs, and they're yours to run:

```powershell
git clone https://github.com/temporalio/samples-server.git
cd samples-server/compose
docker compose up
```

This gives you gRPC on port 7233 and the UI on port 8080. Stop any dev server first, because they share 7233. Then, with no code changes, run:

```powershell
python -m workers.hello_worker
python -m clients.hello_client Omkar
```

The same app, now on a real multi-service cluster. To clean up: `docker compose down -v`.

## 5. Exercise

Draw both diagrams from memory, and write the responsibility table for your own situation.

Then recommend one for each company, with reasons:

1. A four-engineer startup with 50,000 Workflows a day
2. A bank whose regulator forbids sending metadata outside its network
3. A company with a large SRE team, who run Cassandra already, and steady huge volume

## 6. Failure exercise

You self-host. Your persistence database has a two-hour outage. What happens to running Workflows, to Workers, and to timers that should've fired during the outage? What must your backups include? Explain what each answer depends on.

## 7. Review

Type `/check` when you've finished.

## 8. Mastery questions

1. What runs in your infrastructure in both models?
2. What does self-hosting add to your on-call load?
3. Why does the code stay the same?
4. What sees your payloads in Cloud, with and without a codec?
5. Why isn't the dev server suitable for production?

## 9. Gate

Type `/gate`. You'll recommend a model for a new company scenario.

## 10. Next step

**Phase 16: AWS + Temporal.**
