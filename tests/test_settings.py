"""The Jev key and switch are configured from the UI, never echoed back.

Routing must also keep working when there is no key at all — that is the
state a fresh clone starts in, and the rules are what cover it.
"""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from dataflow_agents import routing, web as web_module
from dataflow_agents.orchestrator import load_config
from dataflow_agents.web import create_app

SECRET = "apikey_test_0123456789abcdef"


class SettingsEndpointTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        config_dir = Path(self.tmp.name) / "config"
        config_dir.mkdir()
        (config_dir / "runtime.json").write_text("{}", encoding="utf-8")
        # Redirect every config file the settings endpoints write.
        for name in ("_CONFIG_DIR",):
            self.enterContext(patch.object(web_module, name, config_dir))
        self.config_dir = config_dir
        # An explicit config so the developer's real secret registry is never read.
        self.cfg = load_config(backend="offline", runs_root=self.tmp.name, auto_execute=False)
        # Non-empty so create_app does not fall back to the installed registry.
        self.cfg["resource_secrets"] = {"__test__": "sentinel"}
        self.cfg["use_jev_routing"] = False
        self.client = self.enterContext(TestClient(create_app(self.cfg)))
        # The environment is consulted before the config, so make sure a key
        # from the developer's shell cannot decide the outcome of a test.
        for name in ("DF_USE_JEV_ROUTING", routing.JEV_KEY_ENV):
            self.enterContext(patch.dict("os.environ", {}, clear=False))
            __import__("os").environ.pop(name, None)

    def settings(self):
        return self.client.get("/api/v1/settings").json()["routing"]

    def test_a_fresh_install_has_no_key_and_falls_back_to_rules(self):
        state = self.settings()
        self.assertFalse(state["has_key"])
        self.assertEqual(state["key_source"], "none")
        self.assertEqual(state["effective"], "rules")

    def test_a_key_can_be_saved_and_is_never_returned(self):
        response = self.client.post("/api/v1/settings", json={"api_key": SECRET})
        self.assertEqual(response.status_code, 200)
        state = response.json()["routing"]
        self.assertTrue(state["has_key"])
        self.assertEqual(state["key_source"], "registry")
        self.assertEqual(state["key_hint"], "…" + SECRET[-6:])
        # The raw key must not appear anywhere in what the UI receives.
        self.assertNotIn(SECRET, json.dumps(response.json()))
        self.assertNotIn(SECRET, json.dumps(self.settings()))
        # ...but it was stored, with restrictive permissions.
        stored = json.loads((self.config_dir / "resource-secrets.json").read_text())
        self.assertEqual(stored[routing.JEV_KEY_NAME], SECRET)
        mode = (self.config_dir / "resource-secrets.json").stat().st_mode & 0o777
        self.assertEqual(mode, 0o600)

    def test_toggling_the_model_persists_to_the_runtime_config(self):
        self.client.post("/api/v1/settings", json={"enabled": True})
        self.assertTrue(self.settings()["enabled"])
        self.assertTrue(json.loads((self.config_dir / "runtime.json").read_text())["use_jev_routing"])
        self.client.post("/api/v1/settings", json={"enabled": False})
        self.assertFalse(self.settings()["enabled"])
        self.assertFalse(json.loads((self.config_dir / "runtime.json").read_text())["use_jev_routing"])

    def test_model_routing_needs_both_the_switch_and_a_key(self):
        self.client.post("/api/v1/settings", json={"enabled": True, "api_key": SECRET})
        self.assertEqual(self.settings()["effective"], "jev")
        # Switching off alone is enough to stop calling out.
        self.client.post("/api/v1/settings", json={"enabled": False})
        self.assertEqual(self.settings()["effective"], "rules")
        # ...and so is clearing the key while the switch stays on.
        self.client.post("/api/v1/settings", json={"enabled": True, "api_key": ""})
        self.assertFalse(self.settings()["has_key"])
        self.assertEqual(self.settings()["effective"], "rules")

    def test_a_too_short_key_is_refused(self):
        response = self.client.post("/api/v1/settings", json={"api_key": "short"})
        self.assertEqual(response.status_code, 422)
        self.assertIn("too short", response.json()["detail"])

    def test_a_missing_key_is_not_an_error_on_the_test_endpoint(self):
        response = self.client.post("/api/v1/settings/test", json={})
        self.assertEqual(response.status_code, 422)
        self.assertIn("No API key", response.json()["detail"])

    def test_an_unreachable_model_is_reported_not_raised(self):
        self.client.post("/api/v1/settings", json={"api_key": SECRET})
        with patch.object(routing, "ask_jev",
                          side_effect=routing.JevUnavailable("Jev HTTP 503")):
            body = self.client.post("/api/v1/settings/test", json={}).json()
        self.assertFalse(body["ok"])
        self.assertIn("503", body["error"])

    def test_a_successful_check_reports_the_verdict_and_the_rules_view(self):
        self.client.post("/api/v1/settings", json={"api_key": SECRET})
        with patch.object(routing, "ask_jev", return_value=("status_query", 0.91)):
            body = self.client.post("/api/v1/settings/test", json={"sample": "现在进度怎么样了？"}).json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["intent"], "status_query")
        self.assertEqual(body["confidence"], 0.91)
        self.assertEqual(body["rules_intent"], "status_query")
        self.assertIn("latency_ms", body)


class SecretRegistryIntegrityTests(unittest.TestCase):
    """One credential must never be able to drop another.

    Both writers of the secret registry used to write back the snapshot that
    was loaded at startup, so registering a serving (or saving the routing
    key) replaced every other credential with whatever that process happened
    to have in memory.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        config_dir = Path(self.tmp.name) / "config"
        config_dir.mkdir()
        (config_dir / "runtime.json").write_text("{}", encoding="utf-8")
        (config_dir / "resources.json").write_text("{}", encoding="utf-8")
        # An unrelated credential already on disk, as a pipeline key would be.
        (config_dir / "resource-secrets.json").write_text(
            json.dumps({"llm_default": "sk-existing-pipeline-key"}), encoding="utf-8")
        self.enterContext(patch.object(web_module, "_CONFIG_DIR", config_dir))
        self.config_dir = config_dir
        cfg = load_config(backend="offline", runs_root=self.tmp.name, auto_execute=False)
        # Deliberately stale: this is what the process loaded at startup.
        cfg["resource_secrets"] = {"__stale__": "loaded-at-boot"}
        cfg["resources"] = {}
        self.client = self.enterContext(TestClient(create_app(cfg)))

    def registry(self):
        return json.loads((self.config_dir / "resource-secrets.json").read_text())

    def test_saving_the_routing_key_keeps_other_credentials(self):
        self.client.post("/api/v1/settings", json={"api_key": SECRET})
        stored = self.registry()
        self.assertEqual(stored[routing.JEV_KEY_NAME], SECRET)
        self.assertEqual(stored["llm_default"], "sk-existing-pipeline-key",
                         "saving the routing key dropped the pipeline credential")

    def test_clearing_the_routing_key_keeps_other_credentials(self):
        self.client.post("/api/v1/settings", json={"api_key": SECRET})
        self.client.post("/api/v1/settings", json={"api_key": ""})
        stored = self.registry()
        self.assertNotIn(routing.JEV_KEY_NAME, stored)
        self.assertEqual(stored["llm_default"], "sk-existing-pipeline-key")

    def test_registering_a_serving_keeps_other_credentials(self):
        self.client.post("/api/v1/resources", json={
            "name": "second_serving", "api_url": "https://example.test/v1",
            "model_name": "m", "api_key": "sk-newly-registered"})
        stored = self.registry()
        self.assertEqual(stored["second_serving"], "sk-newly-registered")
        self.assertEqual(stored["llm_default"], "sk-existing-pipeline-key",
                         "registering a serving dropped another credential")


class FallbackTests(unittest.TestCase):
    """With no credential the controller must route without any network call."""

    def test_no_key_means_the_rules_decide(self):
        with patch.object(routing, "ask_jev", side_effect=AssertionError("must not be called")):
            intent, meta = routing.classify_with_source("清洗 raw_content", has_run=False,
                                                        use_model=True, config={"resource_secrets": {}})
        self.assertEqual(intent, "new_task")
        self.assertEqual(meta["by"], "rules")
        self.assertIn("no Jev credential", meta["model_unavailable"])

    def test_an_http_failure_means_the_rules_decide(self):
        with patch.object(routing, "ask_jev", side_effect=routing.JevUnavailable("Jev HTTP 500")):
            intent, meta = routing.classify_with_source("现在进度怎么样了？", has_run=False,
                                                        use_model=True,
                                                        config={"resource_secrets": {routing.JEV_KEY_NAME: "k"}})
        self.assertEqual(intent, "status_query")
        self.assertEqual(meta["by"], "rules")

    def test_a_partial_response_means_the_rules_decide(self):
        # A well-formed HTTP 200 that simply lacks the expected answer.
        with patch.object(routing, "ask_jev", side_effect=routing.JevUnavailable("unknown intent")):
            intent, meta = routing.classify_with_source("改成小写", has_run=True, use_model=True,
                                                        config={"resource_secrets": {routing.JEV_KEY_NAME: "k"}})
        self.assertEqual(intent, "revision")
        self.assertEqual(meta["by"], "rules")


if __name__ == "__main__":
    unittest.main()
