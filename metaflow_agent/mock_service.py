"""
Mock metadata service for demos and tests.

Simulates Metaflow metadata service endpoints with call counting.
Every method increments the internal counter so demos can compare
standard vs agent client HTTP call patterns with real numbers.
"""

import json
import time


class MockMetadataService:
    """Simulates Metaflow metadata service with configurable data volume."""

    def __init__(self, flow_name="TrainingPipeline", n_runs=200,
                 n_steps=10, n_tasks_per_step=100, n_failed=5):
        self.flow_name = flow_name
        self.n_runs = n_runs
        self.n_steps = n_steps
        self.n_tasks = n_tasks_per_step
        self._calls = 0
        self._n_failed_param = n_failed
        self._seed()

    def reset(self):
        self._calls = 0

    @property
    def calls(self):
        return self._calls

    def _seed(self):
        now = int(time.time() * 1000)
        self._now = now

        self.step_names = (
            ["_parameters", "start"]
            + ["process_%d" % i for i in range(self.n_steps - 3)]
            + ["end"]
        )

        self.runs = []
        for i in range(self.n_runs):
            ts = now - i * 60000
            run = {
                "flow_id": self.flow_name,
                "run_number": i + 1,
                "run_id": "run-%d" % (i + 1),
                "ts_epoch": ts,
                "finished_at": ts + 45000,
                "user_name": "trainer",
                "tags": ["production", "experiment-%d" % (i // 20)],
                "system_tags": ["user:trainer", "runtime:prod"],
            }
            if i % 20 == 7:
                run["finished_at"] = None
            self.runs.append(run)

        self._failed_step = self.step_names[-2]
        self._n_failed = self._n_failed_param

    # ── Individual endpoints (standard client pattern) ──────────────

    def get_runs(self):
        """GET /flows/{id}/runs -- unbounded, returns all runs."""
        self._calls += 1
        return list(self.runs)

    def get_run(self, run_id):
        """GET /flows/{id}/runs/{run_id} -- single run."""
        self._calls += 1
        for r in self.runs:
            if r["run_id"] == run_id:
                return dict(r)
        return None

    def get_run_metadata(self, run_id):
        """GET /flows/{id}/runs/{run_id}/metadata"""
        self._calls += 1
        return [{"field_name": "status", "value": "completed", "ts_epoch": 0}]

    def get_run_tags(self, run_id):
        """GET /flows/{id}/runs/{run_id}/tags"""
        self._calls += 1
        for r in self.runs:
            if r["run_id"] == run_id:
                return r["tags"] + r["system_tags"]
        return []

    def get_steps(self, run_id):
        """GET /flows/{id}/runs/{run_id}/steps"""
        self._calls += 1
        return [{"step_name": s, "ts_epoch": 0} for s in self.step_names]

    def get_tasks(self, run_id, step_name):
        """GET /flows/{id}/runs/{run_id}/steps/{step}/tasks"""
        self._calls += 1
        tasks = []
        for t in range(1, self.n_tasks + 1):
            finished = True
            if step_name == self._failed_step and t > self.n_tasks - self._n_failed:
                finished = False
            tasks.append({
                "task_id": t,
                "step_name": step_name,
                "ts_epoch": self._now - 10000,
                "finished_at": self._now if finished else None,
                "system_tags": ["runtime:prod"],
            })
        return tasks

    def get_all_tasks(self, run_id):
        """GET /flows/{id}/runs/{run_id}/tasks -- all tasks across all steps."""
        self._calls += 1
        tasks = []
        for step_name in self.step_names:
            for t in range(1, self.n_tasks + 1):
                finished = True
                if step_name == self._failed_step and t > self.n_tasks - self._n_failed:
                    finished = False
                tasks.append({
                    "task_id": t,
                    "step_name": step_name,
                    "ts_epoch": self._now - 10000,
                    "finished_at": self._now if finished else None,
                    "system_tags": ["runtime:prod"],
                })
        return tasks

    def get_task_artifact(self, run_id, step_name, task_id, artifact_name):
        """GET /flows/{id}/runs/{run_id}/steps/{step}/tasks/{task}/artifacts/{name}"""
        self._calls += 1
        if artifact_name == "_task_ok":
            if step_name == self._failed_step and task_id > self.n_tasks - self._n_failed:
                return None
            return {"name": "_task_ok", "value": "True"}
        if artifact_name == "_exception":
            if step_name == self._failed_step and task_id > self.n_tasks - self._n_failed:
                exceptions = ["ValueError", "RuntimeError", "OOMError",
                              "TimeoutError", "DataCorruptionError"]
                exc = exceptions[task_id % len(exceptions)]
                return {
                    "name": "_exception",
                    "step_name": step_name,
                    "task_id": task_id,
                    "type": exc,
                    "message": "Failed in %s task %d: %s" % (step_name, task_id, exc),
                    "timestamp": "2026-03-28T10:00:%02d" % (task_id % 60),
                }
            return None
        return {"name": artifact_name, "value": "...", "location": "s3://bucket/%s" % artifact_name}

    def get_task_artifacts(self, run_id, step_name, task_id):
        """GET /flows/{id}/runs/{run_id}/steps/{step}/tasks/{task}/artifacts"""
        self._calls += 1
        names = ["_task_ok", "_success", "_foreach_stack", "model", "metrics", "config"]
        return [{"name": n, "location": "s3://bucket/%s" % n} for n in names]

    def get_log_lines(self, run_id, step_name, task_id, log_type="stdout", tail=20):
        """GET /flows/{id}/runs/{run_id}/steps/{step}/tasks/{task}/logs/{type}"""
        self._calls += 1
        return [
            "[2026-03-28 10:00:%02d] Processing batch %d: loss=0.%03d"
            % (i, i, 100 - i)
            for i in range(tail)
        ]

    # ── Batch / paginated endpoints (agent client pattern) ─────────

    def get_runs_paginated(self, limit=50, cursor=None):
        """GET /flows/{id}/runs?_limit=N&_cursor=C -- paginated."""
        self._calls += 1
        start = 0
        if cursor is not None:
            for i, r in enumerate(self.runs):
                if r["ts_epoch"] < cursor:
                    start = i
                    break
        page = self.runs[start:start + limit]
        next_cursor = page[-1]["ts_epoch"] if len(page) == limit and start + limit < len(self.runs) else None
        return page, next_cursor

    def get_runs_paginated_with_tags(self, limit=50, cursor=None, tags=None):
        """GET /flows/{id}/runs?_limit=N&_cursor=C&_tags=T -- paginated + filtered."""
        self._calls += 1
        filtered = self.runs
        if tags:
            filtered = [r for r in self.runs if any(t in r["tags"] for t in tags)]
        start = 0
        if cursor is not None:
            for i, r in enumerate(filtered):
                if r["ts_epoch"] < cursor:
                    start = i
                    break
        page = filtered[start:start + limit]
        next_cursor = page[-1]["ts_epoch"] if len(page) == limit and start + limit < len(filtered) else None
        return page, next_cursor

    def get_run_artifacts_batch(self, run_id):
        """GET /flows/{id}/runs/{run_id}/artifacts -- ALL artifacts for a run."""
        self._calls += 1
        results = []
        for step in self.step_names:
            for t in range(1, self.n_tasks + 1):
                finished = True
                if step == self._failed_step and t > self.n_tasks - self._n_failed:
                    finished = False

                for name in ["_task_ok", "_success", "_exception", "model", "metrics"]:
                    if name == "_task_ok" and not finished:
                        continue
                    if name == "_exception" and finished:
                        continue
                    entry = {
                        "name": name,
                        "step_name": step,
                        "task_id": t,
                        "location": "s3://bucket/%s" % name,
                    }
                    if name == "_exception":
                        exceptions = ["ValueError", "RuntimeError", "OOMError",
                                      "TimeoutError", "DataCorruptionError"]
                        exc = exceptions[t % len(exceptions)]
                        entry["type"] = exc
                        entry["message"] = "Failed in %s task %d: %s" % (step, t, exc)
                        entry["timestamp"] = "2026-03-28T10:00:%02d" % (t % 60)
                    results.append(entry)
        return results

    def get_run_summary_batch(self, run_id):
        """Batch summary -- run + steps + task counts in one call."""
        self._calls += 1
        run = None
        for r in self.runs:
            if r["run_id"] == run_id:
                run = dict(r)
                break
        if not run:
            return None
        run["steps"] = list(self.step_names)
        run["total_tasks"] = len(self.step_names) * self.n_tasks
        run["failed_tasks"] = self._n_failed
        return run

    # ── Payload size helpers (for Sakari demo) ─────────────────────

    def payload_size_bytes(self, data):
        """Compute JSON payload size in bytes."""
        return len(json.dumps(data).encode("utf-8"))
