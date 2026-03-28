"""
Demo: RunBrowser HTTP efficiency vs vanilla Metaflow client.

Run with a real metadata service:
  $ python demo.py MyFlow

Or without a service (uses mock data):
  $ python demo.py --mock
"""

import sys
from datetime import datetime, timedelta

from agent_utils.run_browser import RunBrowser


def mock_demo():
    """Demo with mock provider -- no live service needed."""

    class MockProvider:
        _http_calls = 0

        @classmethod
        def reset_call_count(cls):
            cls._http_calls = 0

        @classmethod
        def get_call_count(cls):
            return cls._http_calls

        @classmethod
        def _get_object_internal(cls, obj_type, obj_order, sub_type,
                                  sub_order, filters, attempt, *args):
            cls._http_calls += 1
            now = int(datetime.now().timestamp() * 1000)
            if sub_type == "self":
                return {
                    "flow_id": "DemoFlow", "run_number": 1,
                    "run_id": "run-1", "ts_epoch": now - 300000,
                    "finished_at": now, "tags": ["demo"],
                    "system_tags": ["runtime:dev"],
                }
            if sub_type == "run":
                return [
                    {
                        "flow_id": "DemoFlow", "run_number": i,
                        "run_id": "run-%d" % i, "user_name": "demo",
                        "ts_epoch": now - i * 60000,
                        "finished_at": now - i * 60000 + 30000,
                        "tags": ["demo", "env:prod" if i % 3 == 0 else "env:dev"],
                        "system_tags": ["runtime:dev"],
                    }
                    for i in range(1, 48)
                ]
            if sub_type == "step":
                return [
                    {"step_name": s, "ts_epoch": now}
                    for s in ["start", "process", "train", "evaluate", "end"]
                ]
            if sub_type == "task":
                return [
                    {
                        "task_id": t, "ts_epoch": now - 10000,
                        "finished_at": now if t < 19 else None,
                        "system_tags": ["runtime:dev"],
                    }
                    for t in range(1, 21)
                ]
            return []

    browser = RunBrowser(provider=MockProvider)

    print("--- list_runs ---")
    runs = browser.list_runs("DemoFlow", limit=10)
    for r in runs[:3]:
        print("  %s  %s  %s" % (r.run_id, r.status, r.created_at.strftime("%H:%M")))
    print("  ... (%d total)" % len(runs))
    print("  HTTP calls: %d" % browser._get_calls())
    print()

    # vanilla comparison: listing 47 runs would be 1 + 47 = 48 calls
    # (1 list call + 1 individual fetch per run for constructor)
    print("  Vanilla Metaflow would make ~48 calls for the same listing")
    print("  RunBrowser: %d call" % browser._get_calls())
    print()

    print("--- diagnose_run ---")
    diag = browser.diagnose_run("DemoFlow/run-1")
    print("  Status: %s" % diag.status)
    print("  Steps: %s" % ", ".join(diag.steps))
    print("  Tasks: %d total, %d failed" % (diag.total_tasks, len(diag.failed_tasks)))
    if diag.failed_tasks:
        for ft in diag.failed_tasks[:3]:
            print("    - %s/task-%s" % (ft.step_name, ft.task_id))
    print("  HTTP calls: %d" % diag.http_calls)
    print()

    # vanilla: 2 (run + flow) + 5 steps * (1 list + 20 tasks * 3 artifact checks) = 302
    print("  Vanilla Metaflow: ~302 calls for 5 steps x 20 tasks")
    print("  RunBrowser: %d calls" % diag.http_calls)


def live_demo(flow_name):
    """Demo against a live metadata service."""
    try:
        from metaflow_extensions.agent.plugins.metadata_providers.agent_service import (
            AgentServiceProvider,
        )
        browser = RunBrowser(provider=AgentServiceProvider)
    except ImportError:
        browser = RunBrowser()

    print("--- list_runs('%s') ---" % flow_name)
    runs = browser.list_runs(flow_name, limit=10)
    if not runs:
        print("  No runs found. Seed some data first.")
        return
    for r in runs[:5]:
        print("  %s  %s  %s" % (r.run_id, r.status, r.created_at.strftime("%Y-%m-%d %H:%M")))
    print("  HTTP calls: %d" % browser._get_calls())
    print()

    print("--- diagnose_run('%s/%s') ---" % (flow_name, runs[0].run_id))
    diag = browser.diagnose_run("%s/%s" % (flow_name, runs[0].run_id))
    print("  Status: %s" % diag.status)
    print("  Steps: %s" % ", ".join(diag.steps))
    print("  Tasks: %d total, %d failed" % (diag.total_tasks, len(diag.failed_tasks)))
    print("  HTTP calls: %d" % diag.http_calls)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--mock":
        mock_demo()
    elif len(sys.argv) > 1:
        live_demo(sys.argv[1])
    else:
        print("Usage: python demo.py <flow_name>")
        print("       python demo.py --mock")
        sys.exit(1)


if __name__ == "__main__":
    main()
