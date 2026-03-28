#!/usr/bin/env python
"""
5-scenario HTTP call comparison: standard Metaflow client vs agent client.

Run:
    cd metaflow-agent
    python demo/demo_comparison.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metaflow_agent.mock_service import MockMetadataService
from metaflow_agent.utils.run_lister import list_runs, list_runs_standard
from metaflow_agent.utils.failure_finder import find_failures, find_failures_standard
from metaflow_agent.utils.run_summary import run_summary, run_summary_standard
from metaflow_agent.utils.artifact_search import search_artifacts, search_artifacts_standard
from metaflow_agent.utils.log_tail import log_tail, log_tail_standard


FLOW = "TrainingPipeline"
N_RUNS = 200
N_STEPS = 10
N_TASKS = 100
SEARCH_RUNS = 10
W = 62  # output width


def fmt_calls(n):
    """Format call count with commas and 'call'/'calls' suffix."""
    s = "{:,}".format(n)
    return "%s %s" % (s, "call" if n == 1 else "calls")


def print_scenario(num, title, std_calls, agent_calls):
    reduction = std_calls // agent_calls if agent_calls > 0 else std_calls
    label = "{:,}x reduction".format(reduction)

    std_str = fmt_calls(std_calls)
    agent_str = fmt_calls(agent_calls)

    print("  %d. %s" % (num, title))
    print("     Current client: %s     Agent client: %s %s"
          % (std_str.rjust(12), agent_str.rjust(12), label.rjust(20)))
    print()
    return std_calls, agent_calls


def main():
    svc = MockMetadataService(
        flow_name=FLOW, n_runs=N_RUNS,
        n_steps=N_STEPS, n_tasks_per_step=N_TASKS,
    )
    run_id = "run-8"  # a failed run (index 7, i%20==7)

    print()
    print("=" * W)
    print("  METAFLOW AGENT CLIENT -- HTTP Call Comparison")
    print("  Scenario: %s | %d runs | %d steps | %d tasks/foreach"
          % (FLOW, N_RUNS, N_STEPS, N_TASKS))
    print("=" * W)
    print()

    total_std = 0
    total_agent = 0

    # Scenario 1: List all runs
    _, std = list_runs_standard(svc)
    _, agent = list_runs(svc, limit=N_RUNS)
    s, a = print_scenario(1, "List all runs for a flow", std, agent)
    total_std += s
    total_agent += a

    # Scenario 2: Find all failed tasks
    _, std = find_failures_standard(svc, run_id)
    _, agent = find_failures(svc, run_id)
    s, a = print_scenario(2, "Find all failed tasks in a run", std, agent)
    total_std += s
    total_agent += a

    # Scenario 3: Run summary
    _, std = run_summary_standard(svc, run_id)
    _, agent = run_summary(svc, run_id)
    s, a = print_scenario(3, "Get run summary", std, agent)
    total_std += s
    total_agent += a

    # Scenario 4: Artifact search across 10 runs
    _, std = search_artifacts_standard(svc, "model", n_runs=SEARCH_RUNS)
    _, agent = search_artifacts(svc, "model", n_runs=SEARCH_RUNS)
    s, a = print_scenario(4,
        "Search for 'model' artifact across %d runs" % SEARCH_RUNS,
        std, agent)
    total_std += s
    total_agent += a

    # Scenario 5: Log tail
    failed_step = svc._failed_step
    _, std = log_tail_standard(svc, run_id, failed_step, svc.n_tasks, lines=20)
    _, agent = log_tail(svc, run_id, failed_step, svc.n_tasks, lines=20)
    s, a = print_scenario(5, "Get last 20 log lines from failed task", std, agent)
    total_std += s
    total_agent += a

    total_reduction = total_std // total_agent if total_agent > 0 else total_std

    print("  " + "-" * (W - 4))
    print("  TOTAL %s %s %s"
          % (fmt_calls(total_std).rjust(24),
             fmt_calls(total_agent).rjust(24),
             ("{:,}x reduction".format(total_reduction)).rjust(20)))
    print("=" * W)
    print()


if __name__ == "__main__":
    main()
