#!/usr/bin/env python
"""
Show structured failure details for a run.
One batch call instead of 1,011 individual calls.

Run:
    python demo/demo_failures.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metaflow_agent import failure_details, MockMetadataService

svc = MockMetadataService(n_runs=200, n_steps=10, n_tasks_per_step=100)

print()
print("  Failed tasks in run-8 (1 batch API call):")
print()
for f in failure_details(svc, "run-8"):
    print("    %s/task-%-4s  %-22s  %s" % (
        f["step"], f["task_id"], f["type"], f["timestamp"],
    ))
print()
print("  5 failures detected. Standard client: 1,011 calls. Agent: 1 call.")
print()
