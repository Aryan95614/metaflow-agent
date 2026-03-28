"""
run_summary -- structured run summary using batch endpoint.

Standard client: get_run + get_steps + get_run_metadata + get_tasks("end") = 4
Agent client: get_run + get_run_summary_batch = 2

2x reduction.
"""


def run_summary(service, run_id):
    """Get structured run summary using batch endpoint.

    Returns (summary_dict, call_count).
    """
    service.reset()
    run = service.get_run(run_id)
    if not run:
        return None, service.calls
    summary = service.get_run_summary_batch(run_id)
    return summary, service.calls


def run_summary_standard(service, run_id):
    """Simulate standard client: individual calls for run, steps, metadata, end tasks.

    Returns (summary_dict, call_count).
    """
    service.reset()
    run = service.get_run(run_id)
    if not run:
        return None, service.calls
    steps = service.get_steps(run_id)
    metadata = service.get_run_metadata(run_id)
    end_tasks = service.get_tasks(run_id, "end")

    summary = dict(run)
    summary["steps"] = [s["step_name"] for s in steps]
    summary["total_tasks"] = len(end_tasks)
    return summary, service.calls
