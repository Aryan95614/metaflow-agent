"""
log_tail -- bounded log retrieval.

Both standard and agent clients make 1 call for direct log access.
This utility exists for completeness and to show that targeted
operations don't need optimization.

1x reduction (already efficient).
"""


def log_tail(service, run_id, step_name, task_id, lines=20):
    """Get last N log lines for a task.

    Returns (log_lines, call_count).
    """
    service.reset()
    logs = service.get_log_lines(run_id, step_name, task_id, tail=lines)
    return logs, service.calls


def log_tail_standard(service, run_id, step_name, task_id, lines=20):
    """Standard client: same single call. Already efficient.

    Returns (log_lines, call_count).
    """
    service.reset()
    logs = service.get_log_lines(run_id, step_name, task_id, tail=lines)
    return logs, service.calls
