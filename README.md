# metaflow-agent

Proof-of-concept: agent-friendly Metaflow client extension with paginated metadata access.

Built on top of the server-side pagination and tag filtering work in [saikonen/metaflow-service#9](https://github.com/saikonen/metaflow-service/pull/9) and [#10](https://github.com/saikonen/metaflow-service/pull/10), plus the client-side auto-pagination prototype at [Aryan95614/metaflow#1](https://github.com/Aryan95614/metaflow/pull/1).

`RunBrowser` provides three methods agents actually call: `list_runs`, `diagnose_run`, and `find_artifacts`. Each tracks HTTP call count to demonstrate the cost reduction vs vanilla Metaflow client operations.

For a run with 5 steps and 100 foreach tasks, `diagnose_run` makes ~7 HTTP calls. The vanilla client path (`Flow('X').runs()` + per-task artifact checks) makes ~302.

## Install

```bash
pip install -e .
```

## Usage

```python
from agent_utils import RunBrowser

browser = RunBrowser()

# bounded listing with tag filter
runs = browser.list_runs("MyFlow", limit=20, tags=["env:prod"])

# structured diagnostic for a single run
diag = browser.diagnose_run("MyFlow/12345")
print(diag.status, diag.failed_tasks, diag.http_calls)

# search for named artifact across recent runs
artifacts = browser.find_artifacts("MyFlow", "model", limit=10)
```

## Demo

```bash
python demo.py --mock    # uses mock data, no service needed
python demo.py MyFlow    # against a live metadata service
```

## GSoC context

- [PR #9](https://github.com/saikonen/metaflow-service/pull/9): cursor-based pagination (server)
- [PR #10](https://github.com/saikonen/metaflow-service/pull/10): server-side tag filtering
- [PR #1](https://github.com/Aryan95614/metaflow/pull/1): client auto-pagination prototype
- [GSoC ideas page](https://github.com/Netflix/metaflow/blob/master/GSOC_2026_PROPOSALS.md)
