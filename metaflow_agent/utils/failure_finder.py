"""
failure_finder -- batch failure detection using the batch artifact endpoint.

Standard client: 1 (list steps) + S (list tasks per step) + S*T (check _task_ok)
Agent client: 1 (batch artifacts for run)

For 10 steps, 100 tasks/step: 1,011 vs 1 (1,011x reduction).
"""


def find_failures(service, run_id):
    """Find all failed tasks in a run using batch artifact endpoint.

    Returns (failed_tasks, call_count).
    """
    service.reset()
    artifacts = service.get_run_artifacts_batch(run_id)

    failed = []
    for a in artifacts:
        if a["name"] == "_exception":
            failed.append({
                "step": a["step_name"],
                "task_id": a["task_id"],
                "type": a.get("type", "Unknown"),
                "message": a.get("message", ""),
                "timestamp": a.get("timestamp", ""),
            })
    return failed, service.calls


def failure_details(service, run_id):
    """Get structured failure details for a run.

    Convenience wrapper around find_failures that returns just the
    failure records. Used in demo scripts.
    """
    failed, _ = find_failures(service, run_id)
    return failed


def find_failures_standard(service, run_id):
    """Simulate standard client: list steps, list tasks, check _task_ok per task.

    The vanilla client checks task.successful (core.py:1549) which triggers
    a per-task artifact GET for _task_ok. For S steps and T tasks/step,
    this is 1 + S + S*T calls.

    Returns (failed_tasks, call_count).
    """
    service.reset()
    steps = service.get_steps(run_id)

    failed = []
    for step in steps:
        step_name = step["step_name"]
        tasks = service.get_tasks(run_id, step_name)
        for task in tasks:
            tid = task["task_id"]
            result = service.get_task_artifact(run_id, step_name, tid, "_task_ok")
            if result is None:
                failed.append({
                    "step": step_name,
                    "task_id": tid,
                })
    return failed, service.calls
