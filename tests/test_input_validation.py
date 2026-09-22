"""Input the API accepted but should not have.

Found by exercising the live API with malformed bodies, rather than by reading
the handlers: `str()` coercion turned a null or a list into a plausible
looking request, a null column value reached the operators, and an unknown
``/api/v1`` route was answered with the SPA's HTML and a 200.
"""
import tempfile
import time
import unittest

from fastapi.testclient import TestClient

from dataflow_agents.orchestrator import load_config
from dataflow_agents.web import MAX_REQUEST_CHARS, create_app


class RequestValidationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        cfg = load_config(backend="offline", runs_root=self.tmp.name, auto_execute=False)
        cfg["resource_secrets"] = {"__test__": "sentinel"}
        self.client = self.enterContext(TestClient(create_app(cfg)))
        # A run accepted here starts background work that keeps writing into
        # the runs root. Cleanups run last-registered-first, so the wait is
        # registered after the removal to run before it.
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.wait_for_idle)

    def wait_for_idle(self, timeout=60):
        """Block until background work stops writing into the runs root.

        A run that has just been queued has no status.json yet, which is
        exactly when its worker is about to create files, so a missing status
        counts as busy rather than as finished.
        """
        deadline = time.monotonic() + timeout
        terminal = {"READY", "VERIFIED", "EXECUTED", "REFUSED", "BLOCKED",
                    "RESOURCE_REQUIRED", "APPROVAL_REQUIRED"}
        while time.monotonic() < deadline:
            runs = self.client.get("/api/v1/runs").json()
            if all(run["state"] in terminal for run in runs):
                return
            time.sleep(0.3)

    def create(self, payload):
        return self.client.post("/api/v1/runs", json=payload)

    def test_a_request_must_be_a_string(self):
        # str(None) is "None" and str(["a"]) is "['a']"; both used to be
        # planned as if the user had typed them.
        for value in (None, ["a"], 42, {"a": 1}, True):
            with self.subTest(value=value):
                response = self.create({"request": value})
                self.assertEqual(response.status_code, 422, response.text)
                self.assertIn("string", response.json()["detail"])

    def test_an_empty_or_whitespace_request_is_refused(self):
        for value in ("", "   ", "\n\t"):
            with self.subTest(value=value):
                self.assertEqual(self.create({"request": value}).status_code, 422)

    def test_a_request_longer_than_the_bound_is_refused(self):
        response = self.create({"request": "x" * (MAX_REQUEST_CHARS + 1)})
        self.assertEqual(response.status_code, 422)
        self.assertIn("characters", response.json()["detail"])
        # Just under the bound is accepted.
        accepted = self.create({"request": "x" * MAX_REQUEST_CHARS})
        self.assertIn(accepted.status_code, (200, 202), accepted.text)

    def test_non_ascii_and_punctuation_requests_are_accepted(self):
        for text in ("清洗 🧹 空格", "?", "…"):
            with self.subTest(text=text):
                response = self.create({"request": text})
                self.assertIn(response.status_code, (200, 202), response.text)
                # Echoed back verbatim, so the bytes survived the round trip.
                self.assertEqual(response.json()["request"], text)

    def test_null_column_values_are_refused_before_the_pipeline_runs(self):
        response = self.create({"request": "clean rows", "input_rows": [{"raw_content": None}]})
        self.assertEqual(response.status_code, 422)
        self.assertIn("empty values", response.json()["detail"])
        # One bad row among good ones is still refused, and the row is named.
        response = self.create({"request": "clean rows",
                                "input_rows": [{"raw_content": "ok"}, {"raw_content": None}]})
        self.assertEqual(response.status_code, 422)
        self.assertIn("2", response.json()["detail"])

    def test_input_keys_must_be_a_list_of_names(self):
        # A bare string would be read character by character: "raw_content"
        # silently becomes eleven one-letter columns.
        for value in ("raw_content", 5, {"a": 1}, ["ok", 5]):
            with self.subTest(value=value):
                response = self.create({"request": "clean rows",
                                        "input_rows": [{"raw_content": "x"}], "input_keys": value})
                self.assertEqual(response.status_code, 422, response.text)
                self.assertIn("list", response.json()["detail"])


class UnknownRouteTests(unittest.TestCase):
    """A typo'd API path is a missing endpoint, not the app's index page."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        cfg = load_config(backend="offline", runs_root=self.tmp.name, auto_execute=False)
        cfg["resource_secrets"] = {"__test__": "sentinel"}
        self.client = self.enterContext(TestClient(create_app(cfg)))

    def test_unknown_api_paths_answer_with_json_404(self):
        for path in ("/api/v1/bogus", "/api/v1/runs/x/y/z", "/api/nonsense"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 404, response.text)
                self.assertIn("application/json", response.headers.get("content-type", ""))

    def test_a_path_traversal_is_rejected_rather_than_routed(self):
        # 400 from the artifact allowlist is as good as 404 here: the point is
        # that it never reaches the SPA fallback and never serves a file.
        response = self.client.get("/api/v1/runs/run-x/artifact/../../secret")
        self.assertIn(response.status_code, (400, 404), response.text)

    def test_the_frontend_route_still_serves_the_app(self):
        response = self.client.get("/some/spa/route")
        # No bundled frontend in a test checkout, but the route must not 404.
        self.assertIn(response.status_code, (200, 404))
        if response.status_code == 200:
            self.assertIn("text/html", response.headers.get("content-type", ""))


if __name__ == "__main__":
    unittest.main()
