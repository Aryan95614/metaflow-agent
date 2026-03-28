"""
run_lister -- bounded run listing using paginated endpoint.

Standard client: 1 unbounded list + 4 per-run metadata calls = 1 + 4N
Agent client: 1 paginated call = 1

For 200 runs: 801 vs 1 (801x reduction).
"""


def list_runs(service, limit=None):
    """List runs using a single paginated call.

    Returns (runs, call_count).
    """
    service.reset()
    fetch_limit = limit if limit else service.n_runs
    runs, _ = service.get_runs_paginated(limit=fetch_limit)
    return runs, service.calls


def list_runs_standard(service):
    """Simulate standard client: unbounded list + per-run metadata fetches.

    The vanilla Metaflow client lists all runs, then for each run the
    MetaflowObject constructor and property access trigger individual
    HTTP calls for metadata, tags, steps, and status.

    Returns (runs, call_count).
    """
    service.reset()
    runs = service.get_runs()
    for run in runs:
        rid = run["run_id"]
        service.get_run(rid)
        service.get_run_metadata(rid)
        service.get_run_tags(rid)
        service.get_steps(rid)
    return runs, service.calls
