# metaflow-agent

Agent-friendly Metaflow client extension with paginated metadata access and batch query utilities.

Built on top of the server-side pagination ([saikonen/metaflow-service#9](https://github.com/saikonen/metaflow-service/pull/9)), tag filtering ([#10](https://github.com/saikonen/metaflow-service/pull/10)), and client auto-pagination ([Aryan95614/metaflow#1](https://github.com/Aryan95614/metaflow/pull/1)).

## The problem

An AI agent diagnosing a failed Metaflow run makes **11,828 HTTP calls** through the standard client (listing runs, checking task status, searching artifacts). The agent utilities collapse this to **16 calls** using paginated endpoints and batch artifact fetching — a **739x reduction**.

| Scenario | Standard | Agent | Reduction |
|---|---|---|---|
| List 200 runs | 801 | 1 | 801x |
| Find failed tasks (10 steps x 100 tasks) | 1,011 | 1 | 1,011x |
| Run summary | 4 | 2 | 2x |
| Search artifact across 10 runs | 10,011 | 11 | 910x |
| Log tail (20 lines) | 1 | 1 | 1x |
| **Total** | **11,828** | **16** | **739x** |

## Install

```bash
pip install -e .
```

## Demo

```bash
python demo/demo_comparison.py    # 5-scenario HTTP call comparison
python demo/demo_sakari.py        # pagination + client + agent impact
```

## Utilities

Five modules in `metaflow_agent/utils/`:

```python
from metaflow_agent import list_runs, failure_details, run_summary, search_artifacts, log_tail
from metaflow_agent import MockMetadataService

svc = MockMetadataService(n_runs=200, n_steps=10, n_tasks_per_step=100)

# bounded run listing (1 paginated call)
runs, calls = list_runs(svc, limit=50)

# batch failure detection (1 batch artifact call)
failures = failure_details(svc, "run-8")

# structured run summary (2 calls)
summary, calls = run_summary(svc, "run-8")

# cross-run artifact search (1 + N batch calls)
results, calls = search_artifacts(svc, "model", n_runs=10)

# bounded log tail (1 call)
lines, calls = log_tail(svc, "run-8", "process_7", 96, lines=20)
```

## Tests

```bash
pytest tests/ -v    # 55 tests
```

## Architecture

The `metaflow_agent` package includes a `MockMetadataService` that simulates both individual endpoints (standard client pattern) and batch/paginated endpoints (agent pattern). Each method call increments an internal counter, enabling accurate HTTP call comparisons.

The five utility modules (`run_lister`, `failure_finder`, `run_summary`, `artifact_search`, `log_tail`) use the batch endpoints. Each module also exports a `*_standard` function that simulates the vanilla Metaflow client's N+1 query pattern for comparison.

## GSoC context

- Server-side: [PR #9](https://github.com/saikonen/metaflow-service/pull/9) (pagination), [PR #10](https://github.com/saikonen/metaflow-service/pull/10) (tag filtering)
- Client: [PR #1](https://github.com/Aryan95614/metaflow/pull/1) (auto-pagination)
- Agent-friendly client: [valayDave/metaflow-service#6](https://github.com/valayDave/metaflow-service/issues/6) (benchmark scenarios)
