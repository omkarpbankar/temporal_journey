# Test Suite Against the Plan

The suite has **163 test functions, which expand to 180 cases** through parametrization.

Run it with `pytest`. It took about 3.5 minutes on this machine. `pytest -m "not integration"` runs the 100 cases that don't need a real server.

## Plan targets (Phase 10)

These count test functions. A test can fall into more than one row, and the rows were counted by matching test names and fixtures, so treat them as close estimates rather than exact figures.

| Category | Plan target | In this suite |
|---|---|---|
| Workflow tests | 20 | 130 |
| Activity tests | 15 | 16 |
| Failure and retry | 5 | 37 |
| Cancellation | 5 | 15 |
| Timer | 5 | 16 |
| Signal, Query, and Update | 5 | 26 |
| Integration (real server) | 5 | 80 cases |

## Feature coverage (Phase 10 topic list)

| Feature | Example test |
|---|---|
| Success path | `test_happy_path` |
| Activity failure | `test_fail_more_than_max_attempts` |
| Retries | `test_fail_three_times_then_succeed` |
| Timeout | `test_hanging_activity_times_out_and_retries` |
| Cancellation | `test_platform_cancelled_during_shipping_refunds_everything` |
| Signal | `test_payment_signal_drives_to_completion` |
| Query | `test_query_reads_state_and_writes_no_history` |
| Update | `test_update_returns_a_value_to_the_caller` |
| Timer | `test_a_simulated_year_takes_seconds` |
| Child Workflow | `test_child_failure_is_compensated_by_the_parent` |
| Replay | `test_saved_production_history_still_replays` |
| Worker crash | `test_worker_killed_after_the_charge_commits` |
| Mocked Activity | `test_mock_an_activity_by_registering_its_name` |

## What these tests don't prove

- Real AWS behaviour. Phase 16 runs against moto, an in-process fake.
- TLS, mTLS, or Temporal Cloud connections.
- A multi-service Temporal cluster. Everything ran on the single-binary dev server.
- Performance under real load.
