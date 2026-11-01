# temporal-learning

The code for every phase of the journey. Lessons are in [../journey/](../journey/README.md).

## Setup

From the repository root, in PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
cd temporal-learning
pytest -m "not integration"     # quick check
```

Everything is already installed in `.venv`. To reinstall: `uv pip install -e ".[dev]"`.

## Layout

```text
src/
├── workflows/     What should happen, deterministically
├── activities/    Everything that touches the outside world
├── workers/       Processes that run them
├── clients/       Processes that start them
├── shared/        Models, connection, ledger, encryption, diagnosis
├── capstone/      The final project
└── my_work/       Yours, for lesson exercises
tests/             One file per phase, plus the capstone
scripts/           Dev server, diagnostics, tracing, codec server
observability/     Prometheus, alerts, and a Grafana dashboard
deploy/            Dockerfile and compose file
```

## Where each phase lives

| Phase | Main code | Tests |
|---|---|---|
| 2 Quickstart | `workflows/hello.py` | `test_phase02_hello.py` |
| 3 Determinism | `workflows/order_basics.py`, `determinism_lab.py` | `test_phase03_determinism.py` |
| 4 Activities | `activities/commerce.py`, `long_running.py`, `workflows/checkout.py` | `test_phase04_*.py` |
| 5 Failures | `workflows/failure_lab.py` | `test_phase05_failures.py` |
| 6 Messages | `workflows/interactive_order.py` | `test_phase06_interactive.py` |
| 7 Long-running | `workflows/subscription.py` | `test_phase07_*.py` |
| 8 Composition | `workflows/composed_order.py` | `test_phase08_composition.py` |
| 9 Scaling | `workflows/routed_order.py`, `workers/specialized.py` | `test_phase09_routing.py` |
| 10 Testing | `scripts/record_history.py`, `tests/histories/` | `test_phase10_testing_toolkit.py` |
| 11 Versioning | `workflows/versioned.py` | `test_phase11_versioning.py` |
| 12 Payloads | `shared/payloads.py`, `object_store.py`, `workflows/payload_lab.py` | `test_phase12_payloads.py` |
| 13 Observability | `shared/diagnose.py`, `workers/observed_worker.py`, `observability/` | `test_phase13_observability.py` |
| 14 Security | `shared/encryption.py`, `scripts/codec_server.py` | `test_phase14_security.py` |
| 15 Deployment models | `shared/connection.py` | `test_phase15_connection.py` |
| 16 AWS | `activities/aws_pipeline.py`, `workflows/aws_pipeline.py` | `test_phase16_aws.py` |
| 17 Data engineering | `activities/data_pipeline.py`, `workflows/data_pipeline.py` | `test_phase17_data_pipeline.py` |
| 18 Patterns | `workflows/patterns.py`, `fulfillment_platform.py`, `workers/platform.py` | `test_phase18_platform.py` |
| 19 Internals | `workflows/internals_lab.py`, `scripts/trace_lifecycle.py` | `test_phase19_internals.py` |
| Capstone | `capstone/`, `deploy/` | `test_capstone.py` |

## Useful commands

```powershell
python scripts/dev_server.py              # Temporal and the UI at localhost:8233
python scripts/trace_lifecycle.py         # Watch replay happen
python scripts/diagnose.py <workflow id>  # Why is this run stuck?
pytest                                    # Everything
pytest -m "not integration"               # Faster subset
```

The first test run downloads the Temporal test servers, so it needs internet. They're cached after that.
