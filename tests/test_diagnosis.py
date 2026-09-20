"""A failed run must be explained by cause, not by whichever error surfaced."""
import json
import tempfile
import unittest
from pathlib import Path

from dataflow_agents.diagnosis import (collect_evidence, failure_digest, triage,
                                       triage_message, analysis_message)


def write(root, name, payload):
    path = Path(root) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


class DiagnosisTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "run-diagnosis"
        self.root.mkdir()
        (self.root / "input.jsonl").write_text('{"raw_content": "  Hello   world  "}\n'
                                               '{"raw_content": "Hello world"}\n', encoding="utf-8")
        write(self.root, "request.json", {"request": "过滤数学问题并生成答案"})
        write(self.root, "status.json", {"state": "BLOCKED"})
        write(self.root, "static-validation.json", {"passed": True, "errors": []})
        write(self.root, "pipeline-spec.json", {
            "steps": [{"operator": "ReasoningQuestionFilter"}, {"operator": "ReasoningAnswerGenerator"}],
            "resources": {"llm_default": {"args": {"api_url": "https://api.example.com/v1/chat/completions",
                                                   "model_name": "gpt-4o",
                                                   "key_name_of_api_key": "DF_PIPELINE_TEST"}}},
            "final_keys": ["raw_content"], "initial_keys": ["raw_content"]})
        (self.root / "pipeline.py").write_text("# generated\n", encoding="utf-8")
        self.config = {"resource_secrets": {"llm_default": "sk-test"}}

    def evidence(self, runtime=None, **status):
        if runtime is not None:
            write(self.root, "runtime-report.json", runtime)
        if status:
            write(self.root, "status.json", status)
        return collect_evidence(self.root, self.config)

    def test_emptied_stage_is_reported_as_an_input_problem(self):
        evidence = self.evidence({"status": "failed", "error_code": "EMPTY_STAGE",
                                  "error": "Step 1 wrote 0 rows",
                                  "underlying_error": "ValueError: Missing required column(s): ['raw_content']",
                                  "stage_rows": [{"step": 1, "rows": 0}]})
        self.assertEqual(evidence["input"], {"rows": 2, "fields": ["raw_content"],
                                             "sample": '{"raw_content": "  Hello   world  "}'})
        verdict = triage(evidence)
        self.assertEqual(verdict["category"], "input_data")
        # It must name the operator that emptied the data, not the symptom.
        self.assertIn("ReasoningQuestionFilter", verdict["title"])
        self.assertIn("raw_content", verdict["summary"])
        self.assertEqual([action["kind"] for action in verdict["actions"]], ["open_input", "revise_request"])
        self.assertIn("换成与需求匹配的输入数据", triage_message(verdict))

    def test_missing_credential_and_unregistered_serving_both_point_at_serving(self):
        for secrets, expected in [({}, "已登记但缺少密钥"), ({"llm_default": "sk-test"}, None)]:
            self.config["resource_secrets"] = secrets
            verdict = triage(self.evidence({"status": "failed", "error": "boom"}))
            if expected:
                self.assertEqual(verdict["category"], "serving")
                self.assertIn(expected, verdict["summary"])
                self.assertEqual(verdict["actions"][0]["kind"], "configure_serving")
            else:
                self.assertNotEqual(verdict["category"], "serving")

        spec = json.loads((self.root / "pipeline-spec.json").read_text())
        spec["resources"]["llm_default"]["args"]["api_url"] = "https://configure-resource.example.invalid/v1"
        write(self.root, "pipeline-spec.json", spec)
        verdict = triage(self.evidence({"status": "resource_required", "resources": ["llm_default"]}))
        self.assertEqual(verdict["category"], "serving")
        self.assertIn("尚未注册", verdict["summary"])

    def test_http_failures_are_serving_not_pipeline(self):
        verdict = triage(self.evidence({"status": "failed",
                                        "error": "RuntimeError: LLM serving llm_default: request failed (404)"}))
        self.assertEqual(verdict["category"], "serving")

    def test_timeout_and_generated_operator_and_generation_are_distinguished(self):
        timeout = triage(self.evidence({"status": "failed", "error_code": "RUNTIME_TIMEOUT",
                                        "error": "Runtime deadline exceeded after 900s"}))
        self.assertEqual(timeout["category"], "timeout")
        self.assertEqual(timeout["actions"][0]["kind"], "reduce_input")

        fixture = triage(self.evidence({"status": "failed",
                                        "error": "AssertionError: Custom fixture failed: step_1/0"}))
        self.assertEqual(fixture["category"], "generated_operator")

        write(self.root, "static-validation.json", {"passed": False, "errors": ["step_2: missing input field answer"]})
        generation = triage(self.evidence({"status": "failed", "error": "; ".join(["step_2: missing input field answer"])}))
        self.assertEqual(generation["category"], "generation")
        self.assertIn("missing input field answer", generation["summary"])

    def test_refusal_is_not_treated_as_a_crash(self):
        verdict = triage(self.evidence(None, state="REFUSED", reason="DataFlow 不支持发送邮件"))
        self.assertEqual(verdict["category"], "refused")
        self.assertIn("邮件", verdict["summary"])

    def test_same_failure_has_a_stable_digest_and_a_different_one_changes(self):
        first = failure_digest(self.evidence({"status": "failed", "error": "boom"}))
        self.assertEqual(first, failure_digest(self.evidence({"status": "failed", "error": "boom"})))
        self.assertNotEqual(first, failure_digest(self.evidence({"status": "failed", "error": "other"})))

    def test_credentials_never_reach_the_explanation(self):
        (self.root / "runtime.stderr.log").write_text("Authorization: Bearer sk-live-should-not-leak\n", encoding="utf-8")
        evidence = self.evidence({"status": "failed", "error": "auth failed with sk-live-should-not-leak"})
        self.assertNotIn("sk-live", json.dumps(evidence, ensure_ascii=False))
        self.assertIn("[REDACTED]", evidence["stderr_tail"])

    def test_model_analysis_is_rendered_with_its_actions(self):
        verdict = triage(self.evidence({"status": "failed", "error": "boom"}))
        text = analysis_message({"cause_category": "input_data", "diagnosis": "输入与需求不符。",
                                 "next_actions": [{"action": "替换数据", "detail": "换成数学题"}],
                                 "regenerate_recommended": True, "suggested_request": "只做去重"}, verdict)
        self.assertIn("输入数据不匹配", text)
        self.assertIn("1. 替换数据 —— 换成数学题", text)
        self.assertIn("只做去重", text)
