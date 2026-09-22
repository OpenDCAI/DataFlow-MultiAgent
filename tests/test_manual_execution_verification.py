"""Manual execution must verify its own output.

Found by an independent red-team review of the merged tree: the button path
(``POST /runs/{id}/execute``, and the rerun endpoint that calls it) recorded
EXECUTED purely on ``runtime["status"] == "passed"``. The orchestrator path
verified; the button did not. A stale or wrong pipeline could therefore emit
malformed or truncated output on new data and be reported as a success.
"""
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from dataflow_agents.orchestrator import Orchestrator, load_config
from dataflow_agents.web import create_app
from test_orchestrator import CustomBackend


class ManualExecutionVerificationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cfg = load_config(backend="offline", runs_root=self.tmp.name, auto_execute=False)
        self.cfg.update(resources={}, resource_secrets={"test": "unused-test-secret"})
        self.app = create_app(self.cfg)
        self.client = self.enterContext(TestClient(self.app))
        self.root = Path(self.tmp.name) / "run-manual"
        with patch("dataflow_agents.web.Orchestrator",
                   side_effect=lambda config: Orchestrator(config=config, backend=CustomBackend())):
            Orchestrator(config=self.cfg, backend=CustomBackend()).run("Count A", run_dir=self.root)
        self.spec = json.loads((self.root / "pipeline-spec.json").read_text())

    def execute_with_report(self, report, rows=None):
        """Run the execute worker inline with a stubbed runtime report."""
        routes = {(route.path, method): route.endpoint for route in self.app.routes
                  for method in getattr(route, "methods", [])}
        queued = []
        executor = self.app.state.executor
        with patch.object(executor, "submit", side_effect=lambda fn: queued.append(fn)):
            routes["/api/v1/runs/{run_id}/execute", "POST"]("run-manual")
            if rows is not None:
                (self.root / "candidate.jsonl").write_text(
                    "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
            with patch("dataflow_agents.web.execute", return_value=report):
                queued.pop(0)()
        return json.loads((self.root / "status.json").read_text())

    def good_rows(self, count=2):
        return [dict.fromkeys(self.spec["final_keys"], "x") for _ in range(count)]

    def good_report(self, rows):
        return {"status": "passed", "compile": True, "executed": True,
                "rows": len(rows), "fields": self.spec["final_keys"]}

    def test_a_clean_execution_is_verified_and_recorded(self):
        rows = self.good_rows()
        status = self.execute_with_report(self.good_report(rows), rows)
        self.assertEqual(status["state"], "EXECUTED", status)
        self.assertEqual(status["verification_mode"], "fields")
        verdict = json.loads((self.root / "verification.json").read_text())
        self.assertEqual(verdict["verdict"], "pass")
        self.assertEqual(verdict["source"], "manual_execution")
        # A verified run carries integrity hashes for what it produced.
        self.assertTrue((self.root / "integrity.json").exists())
        self.assertTrue((self.root / "output.jsonl").exists())

    def test_a_report_claiming_rows_it_never_wrote_fails(self):
        """The exact shape that used to pass: passed, with no output file."""
        status = self.execute_with_report({"status": "passed", "compile": True, "executed": True,
                                           "rows": 2, "fields": self.spec["final_keys"]}, rows=None)
        self.assertEqual(status["state"], "BLOCKED", status)
        self.assertIn("输出文件", status["error"])

    def test_missing_compile_or_execute_flags_fail(self):
        for report in ({"status": "passed", "rows": 2, "fields": self.spec["final_keys"]},
                       {"status": "passed", "compile": True, "rows": 2, "fields": self.spec["final_keys"]}):
            with self.subTest(report=report):
                # Reset the run so each case starts from a terminal state.
                (self.root / "candidate.jsonl").unlink(missing_ok=True)
                status = self.execute_with_report(report, self.good_rows())
                self.assertEqual(status["state"], "BLOCKED", status)

    def test_missing_fields_or_a_row_count_mismatch_fail(self):
        rows = self.good_rows()
        # A field the pipeline promised but the report does not show is missing.
        missing = self.good_report(rows) | {"fields": self.spec["final_keys"][:1]}
        status = self.execute_with_report(missing, rows)
        self.assertEqual(status["state"], "BLOCKED", status)

        rows = self.good_rows()
        short = self.good_report(rows) | {"rows": len(rows) + 5}
        status = self.execute_with_report(short, rows)
        self.assertEqual(status["state"], "BLOCKED", status)
        self.assertIn("行数", status["error"])

    def test_an_extra_column_in_the_report_alone_is_tolerated(self):
        """The delivered rows are the contract, not the diagnostic report.

        The runtime schema is reported before final projection, so it can list
        columns the delivered file legitimately lacks. Delivered rows carrying
        an extra column still fail (covered above); the report merely naming
        one does not, because nothing wrong reached the output file.
        """
        rows = self.good_rows()
        extra = self.good_report(rows) | {"fields": self.spec["final_keys"] + ["dedup_label"]}
        status = self.execute_with_report(extra, rows)
        self.assertEqual(status["state"], "EXECUTED", status)

    def test_rows_carrying_extra_columns_fail(self):
        rows = [dict.fromkeys(self.spec["final_keys"], "x") | {"extra": 1} for _ in range(2)]
        status = self.execute_with_report(self.good_report(rows), rows)
        self.assertEqual(status["state"], "BLOCKED", status)

    def test_a_row_preservation_request_that_loses_rows_fails(self):
        """A rerun on data the pipeline shrinks must not report success."""
        request = json.loads((self.root / "request.json").read_text())
        request["request"] = "保留所有原始记录，统计后输出"
        request["constraints"] = {"preserve_all_rows": True}
        request["input_rows"] = 6
        (self.root / "request.json").write_text(json.dumps(request, ensure_ascii=False), encoding="utf-8")
        rows = self.good_rows(2)          # 6 in, 2 out
        status = self.execute_with_report(self.good_report(rows), rows)
        self.assertEqual(status["state"], "BLOCKED", status)
        self.assertIn("保留", status["error"])

    def test_a_failed_runtime_still_blocks(self):
        (self.root / "candidate.jsonl").unlink(missing_ok=True)
        status = self.execute_with_report({"status": "failed", "compile": True, "executed": False,
                                           "error": "ValueError: boom"}, rows=None)
        self.assertEqual(status["state"], "BLOCKED", status)
        self.assertIn("boom", status["error"])


if __name__ == "__main__":
    unittest.main()
