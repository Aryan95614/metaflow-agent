"""
artifact_search -- cross-run artifact search using batch endpoints.

Standard client: 1 (list runs) + R (list all tasks per run) + R*T_total (artifacts per task)
Agent client: 1 (list runs paginated) + R (batch artifacts per run)

For 10 runs, 10 steps, 100 tasks/step: 10,011 vs 11 (910x reduction).
"""


def search_artifacts(service, artifact_name, n_runs=10):
    """Search for a named artifact across recent runs using batch endpoints.

    Returns (results, call_count).
    """
    service.reset()
    runs, _ = service.get_runs_paginated(limit=n_runs)

    results = []
    for run in runs:
        rid = run["run_id"]
        artifacts = service.get_run_artifacts_batch(rid)
        for a in artifacts:
            if a["name"] == artifact_name:
                results.append({
                    "run_id": rid,
                    "step": a["step_name"],
                    "task_id": a["task_id"],
                    "location": a.get("location", ""),
                })
    return results, service.calls


def search_artifacts_standard(service, artifact_name, n_runs=10):
    """Simulate standard client: list runs, list tasks per run, check artifacts per task.

    The vanilla client iterates each run, lists all tasks (1 call per run
    returning all tasks across steps), then checks artifacts per task
    (1 call per task). For R runs with T total tasks each:
    1 + R + R*T calls.

    Returns (results, call_count).
    """
    service.reset()
    all_runs = service.get_runs()
    runs_to_search = all_runs[:n_runs]

    results = []
    for run in runs_to_search:
        rid = run["run_id"]
        tasks = service.get_all_tasks(rid)
        for task in tasks:
            artifacts = service.get_task_artifacts(
                rid, task["step_name"], task["task_id"],
            )
            for a in artifacts:
                if a["name"] == artifact_name:
                    results.append({
                        "run_id": rid,
                        "step": task["step_name"],
                        "task_id": task["task_id"],
                        "location": a.get("location", ""),
                    })
    return results, service.calls
