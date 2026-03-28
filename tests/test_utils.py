import unittest
from metaflow_agent.mock_service import MockMetadataService
from metaflow_agent.utils.run_lister import list_runs, list_runs_standard
from metaflow_agent.utils.failure_finder import (
    find_failures, find_failures_standard, failure_details,
)
from metaflow_agent.utils.run_summary import run_summary, run_summary_standard
from metaflow_agent.utils.artifact_search import (
    search_artifacts, search_artifacts_standard,
)
from metaflow_agent.utils.log_tail import log_tail, log_tail_standard


# ── Test parameters matching demo scenarios ───────────────────────

N_RUNS = 200
N_STEPS = 10
N_TASKS = 100
SEARCH_RUNS = 10


class TestRunLister(unittest.TestCase):

    def setUp(self):
        self.svc = MockMetadataService(
            n_runs=N_RUNS, n_steps=N_STEPS, n_tasks_per_step=N_TASKS,
        )

    def test_agent_call_count(self):
        _, calls = list_runs(self.svc, limit=N_RUNS)
        self.assertEqual(calls, 1)

    def test_standard_call_count(self):
        _, calls = list_runs_standard(self.svc)
        # 1 (list) + 200 * 4 (get_run, metadata, tags, steps) = 801
        self.assertEqual(calls, 1 + N_RUNS * 4)

    def test_agent_returns_runs(self):
        runs, _ = list_runs(self.svc, limit=10)
        self.assertEqual(len(runs), 10)

    def test_standard_returns_all_runs(self):
        runs, _ = list_runs_standard(self.svc)
        self.assertEqual(len(runs), N_RUNS)


class TestFailureFinder(unittest.TestCase):

    def setUp(self):
        self.svc = MockMetadataService(
            n_runs=N_RUNS, n_steps=N_STEPS, n_tasks_per_step=N_TASKS,
        )
        self.run_id = "run-8"

    def test_agent_call_count(self):
        _, calls = find_failures(self.svc, self.run_id)
        self.assertEqual(calls, 1)

    def test_standard_call_count(self):
        _, calls = find_failures_standard(self.svc, self.run_id)
        # 1 (steps) + 10 (tasks/step) + 10*100 (_task_ok checks) = 1,011
        self.assertEqual(calls, 1 + N_STEPS + N_STEPS * N_TASKS)

    def test_agent_finds_failures(self):
        failed, _ = find_failures(self.svc, self.run_id)
        self.assertEqual(len(failed), self.svc._n_failed)

    def test_standard_finds_failures(self):
        failed, _ = find_failures_standard(self.svc, self.run_id)
        self.assertEqual(len(failed), self.svc._n_failed)

    def test_failure_details_structure(self):
        details = failure_details(self.svc, self.run_id)
        self.assertEqual(len(details), self.svc._n_failed)
        for d in details:
            self.assertIn("step", d)
            self.assertIn("task_id", d)
            self.assertIn("type", d)
            self.assertIn("message", d)
            self.assertIn("timestamp", d)


class TestRunSummary(unittest.TestCase):

    def setUp(self):
        self.svc = MockMetadataService(
            n_runs=N_RUNS, n_steps=N_STEPS, n_tasks_per_step=N_TASKS,
        )
        self.run_id = "run-8"

    def test_agent_call_count(self):
        _, calls = run_summary(self.svc, self.run_id)
        self.assertEqual(calls, 2)

    def test_standard_call_count(self):
        _, calls = run_summary_standard(self.svc, self.run_id)
        self.assertEqual(calls, 4)

    def test_agent_returns_summary(self):
        summary, _ = run_summary(self.svc, self.run_id)
        self.assertIsNotNone(summary)
        self.assertIn("steps", summary)
        self.assertIn("total_tasks", summary)

    def test_missing_run(self):
        summary, _ = run_summary(self.svc, "run-99999")
        self.assertIsNone(summary)


class TestArtifactSearch(unittest.TestCase):

    def setUp(self):
        self.svc = MockMetadataService(
            n_runs=N_RUNS, n_steps=N_STEPS, n_tasks_per_step=N_TASKS,
        )

    def test_agent_call_count(self):
        _, calls = search_artifacts(self.svc, "model", n_runs=SEARCH_RUNS)
        # 1 (paginated list) + 10 (batch per run) = 11
        self.assertEqual(calls, 1 + SEARCH_RUNS)

    def test_standard_call_count(self):
        _, calls = search_artifacts_standard(self.svc, "model", n_runs=SEARCH_RUNS)
        # 1 (list) + 10 (all_tasks per run) + 10*1000 (artifacts per task) = 10,011
        total_tasks_per_run = self.svc.n_steps * self.svc.n_tasks
        expected = 1 + SEARCH_RUNS + SEARCH_RUNS * total_tasks_per_run
        self.assertEqual(calls, expected)

    def test_agent_finds_artifacts(self):
        results, _ = search_artifacts(self.svc, "model", n_runs=SEARCH_RUNS)
        self.assertGreater(len(results), 0)
        for r in results[:3]:
            self.assertIn("run_id", r)
            self.assertIn("location", r)

    def test_nonexistent_artifact(self):
        results, _ = search_artifacts(self.svc, "nonexistent", n_runs=5)
        self.assertEqual(len(results), 0)


class TestLogTail(unittest.TestCase):

    def setUp(self):
        self.svc = MockMetadataService(
            n_runs=N_RUNS, n_steps=N_STEPS, n_tasks_per_step=N_TASKS,
        )

    def test_agent_call_count(self):
        _, calls = log_tail(self.svc, "run-8", "start", 1, lines=20)
        self.assertEqual(calls, 1)

    def test_standard_call_count(self):
        _, calls = log_tail_standard(self.svc, "run-8", "start", 1, lines=20)
        self.assertEqual(calls, 1)

    def test_returns_correct_line_count(self):
        lines, _ = log_tail(self.svc, "run-8", "start", 1, lines=20)
        self.assertEqual(len(lines), 20)


class TestScenarioTotals(unittest.TestCase):
    """Verify the exact numbers shown in demo_comparison.py output."""

    def setUp(self):
        self.svc = MockMetadataService(
            n_runs=N_RUNS, n_steps=N_STEPS, n_tasks_per_step=N_TASKS,
        )
        self.run_id = "run-8"

    def test_total_standard_calls(self):
        _, s1 = list_runs_standard(self.svc)
        _, s2 = find_failures_standard(self.svc, self.run_id)
        _, s3 = run_summary_standard(self.svc, self.run_id)
        _, s4 = search_artifacts_standard(self.svc, "model", n_runs=SEARCH_RUNS)
        _, s5 = log_tail_standard(
            self.svc, self.run_id, self.svc._failed_step, self.svc.n_tasks,
        )
        total = s1 + s2 + s3 + s4 + s5
        self.assertEqual(total, 11828)

    def test_total_agent_calls(self):
        _, a1 = list_runs(self.svc, limit=N_RUNS)
        _, a2 = find_failures(self.svc, self.run_id)
        _, a3 = run_summary(self.svc, self.run_id)
        _, a4 = search_artifacts(self.svc, "model", n_runs=SEARCH_RUNS)
        _, a5 = log_tail(
            self.svc, self.run_id, self.svc._failed_step, self.svc.n_tasks,
        )
        total = a1 + a2 + a3 + a4 + a5
        self.assertEqual(total, 16)

    def test_individual_standard_counts(self):
        _, s1 = list_runs_standard(self.svc)
        self.assertEqual(s1, 801)

        _, s2 = find_failures_standard(self.svc, self.run_id)
        self.assertEqual(s2, 1011)

        _, s3 = run_summary_standard(self.svc, self.run_id)
        self.assertEqual(s3, 4)

        _, s4 = search_artifacts_standard(self.svc, "model", n_runs=SEARCH_RUNS)
        self.assertEqual(s4, 10011)

        _, s5 = log_tail_standard(
            self.svc, self.run_id, self.svc._failed_step, self.svc.n_tasks,
        )
        self.assertEqual(s5, 1)

    def test_individual_agent_counts(self):
        _, a1 = list_runs(self.svc, limit=N_RUNS)
        self.assertEqual(a1, 1)

        _, a2 = find_failures(self.svc, self.run_id)
        self.assertEqual(a2, 1)

        _, a3 = run_summary(self.svc, self.run_id)
        self.assertEqual(a3, 2)

        _, a4 = search_artifacts(self.svc, "model", n_runs=SEARCH_RUNS)
        self.assertEqual(a4, 11)

        _, a5 = log_tail(
            self.svc, self.run_id, self.svc._failed_step, self.svc.n_tasks,
        )
        self.assertEqual(a5, 1)


if __name__ == "__main__":
    unittest.main()
