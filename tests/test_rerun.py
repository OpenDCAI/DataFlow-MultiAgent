"""Re-running a pipeline against new data reuses the pipeline, not the agents."""
import json
import shutil
import tempfile
import time
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from dataflow_agents import web as web_module
from dataflow_agents.orchestrator import Orchestrator, load_config
from dataflow_agents.web import create_app


class RerunTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        # The dataset registry lives in the installed config directory, so a
        # test that registers one must point it at a scratch copy: otherwise
        # every suite run leaves rows in the real workbench.
        self.config_dir = Path(self.tmp.name) / "config"
        self.config_dir.mkdir()
        original = Path(web_module.__file__).parents[1] / "config" / "datasets.json"
        if original.exists():
            shutil.copyfile(original, self.config_dir / "datasets.json")
        self.addCleanup(setattr, web_module, "_CONFIG_DIR", web_module._CONFIG_DIR)
        web_module._CONFIG_DIR = self.config_dir
        self.cfg = load_config(backend="offline", runs_root=self.tmp.name, auto_execute=False)
        self.client = self.enterContext(TestClient(create_app(self.cfg)))
        self.root = Path(self.tmp.name) / "run-source"
        Orchestrator(config=self.cfg).run("清洗空格并去重", run_dir=self.root)
        self.spec = json.loads((self.root / "pipeline-spec.json").read_text())
        # A rerun executes in the background; let it finish before the
        # temporary directory it is writing into is removed (LIFO cleanup).
        self.addCleanup(self.wait_for_idle)

    def wait_for_idle(self, timeout=60):
        deadline = time.monotonic() + timeout
        busy = {"QUEUED", "RUNNING", "PLANNING", "BINDING", "INTEGRATING", "VALIDATING"}
        while time.monotonic() < deadline:
            # A directory without status.json was never started; the API
            # reports it as QUEUED, which would never resolve.
            states = {run["state"] for run in self.client.get("/api/v1/runs").json()
                      if (Path(self.tmp.name) / run["run_id"] / "status.json").exists()}
            if not states & busy:
                return
            time.sleep(0.4)
        raise AssertionError(f"runs still busy after {timeout}s")

    def rerun(self, payload):
        return self.client.post("/api/v1/runs/run-source/rerun", json=payload)

    def test_new_data_gets_a_new_run_that_reuses_the_validated_spec(self):
        rows = [{"raw_content": "  fresh   text  "}, {"raw_content": "fresh text"}]
        response = self.rerun({"input_rows": rows})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["parent_run_id"], "run-source")
        self.assertEqual(body["rows"], 2)

        child = Path(self.tmp.name) / body["run_id"]
        # Same pipeline, new data, and no agent was consulted to get there.
        self.assertEqual(json.loads((child / "pipeline-spec.json").read_text()), self.spec)
        self.assertEqual([json.loads(line) for line in
                          (child / "input.jsonl").read_text().splitlines() if line.strip()], rows)
        self.assertFalse((child / "agents").exists())
        self.assertTrue((child / "pipeline.py").exists())
        self.assertTrue((child / "run_pipeline.py").exists())
        self.assertEqual(json.loads((child / "revision.json").read_text())["parent_run_id"], "run-source")
        # The parent keeps its own snapshot untouched.
        self.assertNotEqual((child / "input.jsonl").read_text(), (self.root / "input.jsonl").read_text())

    def test_data_the_pipeline_cannot_read_is_refused_before_execution(self):
        response = self.rerun({"input_rows": [{"question": "1 + 1?"}]})
        self.assertEqual(response.status_code, 422)
        self.assertIn("raw_content", response.json()["detail"])
        # A partially matching batch is refused too: every row must carry the column.
        response = self.rerun({"input_rows": [{"raw_content": "ok"}, {"other": "missing"}]})
        self.assertEqual(response.status_code, 422)
        for payload in ({"input_rows": []}, {"input_rows": "text"}, {}):
            self.assertEqual(self.rerun(payload).status_code, 422)
        self.assertEqual(self.rerun({"dataset_id": "ds-missing"}).status_code, 404)

    def test_a_run_without_a_pipeline_cannot_be_reused(self):
        empty = Path(self.tmp.name) / "run-empty"
        empty.mkdir()
        response = self.client.post("/api/v1/runs/run-empty/rerun", json={"input_rows": [{"raw_content": "x"}]})
        self.assertEqual(response.status_code, 409)

    def test_registered_dataset_is_read_from_disk(self):
        created = self.client.post("/api/v1/datasets", json={
            "name": "rerun demo", "rows": [{"raw_content": "  a  "}, {"raw_content": "b"}, {"raw_content": "b"}]}).json()
        body = self.rerun({"dataset_id": created["id"]}).json()
        self.assertEqual(body["rows"], 3)
        child = Path(self.tmp.name) / body["run_id"]
        self.assertEqual(json.loads((child / "revision.json").read_text())["dataset_id"], created["id"])
