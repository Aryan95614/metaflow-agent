import unittest
from datetime import datetime

from agent_utils.run_browser import RunBrowser, RunSummary, RunDiagnostic


def make_run(run_number, ts_offset=0, finished=True, tags=None):
    base_ts = 1700000000000
    ts = base_ts - ts_offset * 1000
    return {
        "flow_id": "TestFlow",
        "run_number": run_number,
        "run_id": "run-%d" % run_number,
        "user_name": "test",
        "ts_epoch": ts,
        "finished_at": ts + 60000 if finished else None,
        "tags": tags or ["test"],
        "system_tags": ["runtime:dev"],
    }


def make_step(step_name):
    return {"step_name": step_name, "ts_epoch": 1700000000000}


def make_task(task_id, finished=True):
    return {
        "task_id": task_id,
        "ts_epoch": 1700000000000,
        "finished_at": 1700000060000 if finished else None,
        "system_tags": ["runtime:dev"],
    }


def make_provider(response_fn):
    """Build a mock provider with a custom response function.
    response_fn(obj_type, sub_type, args) -> list or dict
    """
    class Provider:
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
            return response_fn(obj_type, sub_type, args)

    return Provider


class TestListRuns(unittest.TestCase):

    def test_list_runs_bounded(self):
        runs = [make_run(i, ts_offset=i) for i in range(50)]
        provider = make_provider(lambda ot, st, a: runs)
        browser = RunBrowser(provider=provider)

        result = browser.list_runs("TestFlow", limit=10)
        self.assertEqual(len(result), 10)
        self.assertIsInstance(result[0], RunSummary)

    def test_list_runs_with_tags(self):
        runs = [make_run(1, tags=["env:prod"])]
        provider = make_provider(lambda ot, st, a: runs)
        browser = RunBrowser(provider=provider)

        result = browser.list_runs("TestFlow", tags=["env:prod"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].tags, ["env:prod"])

    def test_list_runs_empty_flow(self):
        provider = make_provider(lambda ot, st, a: None)
        browser = RunBrowser(provider=provider)

        result = browser.list_runs("EmptyFlow")
        self.assertEqual(result, [])


class TestDiagnoseRun(unittest.TestCase):

    def test_diagnose_run_call_count(self):
        steps = ["start", "process", "join", "validate", "end"]
        step_data = [make_step(s) for s in steps]
        task_data = [make_task(i) for i in range(1, 21)]
        run = make_run(1)

        def respond(obj_type, sub_type, args):
            if sub_type == "self":
                return run
            if sub_type == "step":
                return step_data
            if sub_type == "task":
                return task_data
            return []

        provider = make_provider(respond)
        browser = RunBrowser(provider=provider)

        result = browser.diagnose_run("TestFlow/run-1")
        # 1 (run) + 1 (steps) + 5 (tasks per step) = 7
        self.assertLessEqual(result.http_calls, 10)
        self.assertEqual(result.total_tasks, 100)

    def test_diagnose_run_finds_failures(self):
        run = make_run(1, finished=False)
        steps = [make_step("start"), make_step("process")]
        good_tasks = [make_task(1, finished=True)]
        bad_tasks = [make_task(2, finished=False)]

        def respond(obj_type, sub_type, args):
            if sub_type == "self":
                return run
            if sub_type == "step":
                return steps
            if sub_type == "task":
                step = args[-1] if args else ""
                if step == "process":
                    return bad_tasks
                return good_tasks
            return []

        provider = make_provider(respond)
        browser = RunBrowser(provider=provider)

        result = browser.diagnose_run("TestFlow/run-1")
        self.assertEqual(len(result.failed_tasks), 1)
        self.assertEqual(result.failed_tasks[0].step_name, "process")
        self.assertEqual(result.status, "failed")


class TestFindArtifacts(unittest.TestCase):

    def test_find_artifacts_returns_metadata(self):
        runs = [make_run(1)]
        end_tasks = [make_task(1)]
        artifacts = [
            {"name": "model", "location": "s3://bucket/model.pkl"},
            {"name": "metrics", "location": "s3://bucket/metrics.json"},
        ]

        def respond(obj_type, sub_type, args):
            if sub_type == "run":
                return runs
            if sub_type == "task":
                return end_tasks
            if sub_type == "artifact":
                return artifacts
            return []

        provider = make_provider(respond)
        browser = RunBrowser(provider=provider)

        result = browser.find_artifacts("TestFlow", "model", limit=5)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["artifact_name"], "model")
        self.assertEqual(result[0]["location"], "s3://bucket/model.pkl")

    def test_fallback_without_pagination(self):
        runs = [make_run(1)]
        provider = make_provider(
            lambda ot, st, a: runs if st == "run" else []
        )
        browser = RunBrowser(provider=provider)

        result = browser.list_runs("TestFlow")
        self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()
