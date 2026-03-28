import unittest
from metaflow_agent.mock_service import MockMetadataService


class TestMockServiceData(unittest.TestCase):

    def setUp(self):
        self.svc = MockMetadataService(
            flow_name="TestFlow", n_runs=50,
            n_steps=5, n_tasks_per_step=20,
        )

    def test_run_count(self):
        self.assertEqual(len(self.svc.runs), 50)

    def test_step_count(self):
        # _parameters + start + (n_steps - 3) process steps + end
        self.assertEqual(len(self.svc.step_names), 5)

    def test_run_fields(self):
        run = self.svc.runs[0]
        for key in ("flow_id", "run_number", "run_id", "ts_epoch",
                     "finished_at", "user_name", "tags", "system_tags"):
            self.assertIn(key, run)

    def test_failed_runs_exist(self):
        failed = [r for r in self.svc.runs if r["finished_at"] is None]
        self.assertGreater(len(failed), 0)


class TestMockServiceCallCounting(unittest.TestCase):

    def setUp(self):
        self.svc = MockMetadataService(n_runs=10, n_steps=3, n_tasks_per_step=5)

    def test_initial_count_zero(self):
        self.assertEqual(self.svc.calls, 0)

    def test_get_runs_increments(self):
        self.svc.get_runs()
        self.assertEqual(self.svc.calls, 1)

    def test_reset_clears_count(self):
        self.svc.get_runs()
        self.svc.get_runs()
        self.svc.reset()
        self.assertEqual(self.svc.calls, 0)

    def test_multiple_calls_accumulate(self):
        self.svc.get_runs()
        self.svc.get_run("run-1")
        self.svc.get_steps("run-1")
        self.assertEqual(self.svc.calls, 3)


class TestMockServiceEndpoints(unittest.TestCase):

    def setUp(self):
        self.svc = MockMetadataService(n_runs=20, n_steps=5, n_tasks_per_step=10)

    def test_get_runs_returns_all(self):
        runs = self.svc.get_runs()
        self.assertEqual(len(runs), 20)

    def test_get_run_found(self):
        run = self.svc.get_run("run-1")
        self.assertIsNotNone(run)
        self.assertEqual(run["run_id"], "run-1")

    def test_get_run_not_found(self):
        run = self.svc.get_run("run-999")
        self.assertIsNone(run)

    def test_get_steps(self):
        steps = self.svc.get_steps("run-1")
        self.assertEqual(len(steps), 5)

    def test_get_tasks(self):
        tasks = self.svc.get_tasks("run-1", "start")
        self.assertEqual(len(tasks), 10)

    def test_get_all_tasks(self):
        tasks = self.svc.get_all_tasks("run-1")
        self.assertEqual(len(tasks), 50)  # 5 steps * 10 tasks

    def test_get_task_artifact_found(self):
        a = self.svc.get_task_artifact("run-1", "start", 1, "model")
        self.assertIsNotNone(a)
        self.assertEqual(a["name"], "model")

    def test_get_task_artifact_task_ok_failed(self):
        # Failed tasks don't have _task_ok
        failed_step = self.svc._failed_step
        failed_tid = self.svc.n_tasks  # last task, should be failed
        a = self.svc.get_task_artifact("run-1", failed_step, failed_tid, "_task_ok")
        self.assertIsNone(a)

    def test_get_log_lines(self):
        lines = self.svc.get_log_lines("run-1", "start", 1, tail=10)
        self.assertEqual(len(lines), 10)

    def test_paginated_limit(self):
        page, cursor = self.svc.get_runs_paginated(limit=5)
        self.assertEqual(len(page), 5)
        self.assertIsNotNone(cursor)

    def test_paginated_cursor_advances(self):
        p1, cursor1 = self.svc.get_runs_paginated(limit=5)
        p2, cursor2 = self.svc.get_runs_paginated(limit=5, cursor=cursor1)
        self.assertNotEqual(p1[0]["run_id"], p2[0]["run_id"])

    def test_paginated_last_page_no_cursor(self):
        page, cursor = self.svc.get_runs_paginated(limit=100)
        self.assertIsNone(cursor)

    def test_batch_artifacts(self):
        arts = self.svc.get_run_artifacts_batch("run-1")
        self.assertGreater(len(arts), 0)
        names = set(a["name"] for a in arts)
        self.assertIn("model", names)

    def test_batch_artifacts_has_exceptions(self):
        arts = self.svc.get_run_artifacts_batch("run-1")
        exceptions = [a for a in arts if a["name"] == "_exception"]
        self.assertEqual(len(exceptions), self.svc._n_failed)

    def test_run_summary_batch(self):
        summary = self.svc.get_run_summary_batch("run-1")
        self.assertIsNotNone(summary)
        self.assertIn("steps", summary)
        self.assertIn("total_tasks", summary)
        self.assertIn("failed_tasks", summary)

    def test_payload_size(self):
        runs = self.svc.get_runs()
        size = self.svc.payload_size_bytes(runs)
        self.assertGreater(size, 0)


if __name__ == "__main__":
    unittest.main()
