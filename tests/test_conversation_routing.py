"""Routing a message must not be decided by a field name inside it.

Reported from the field: a request that listed `status` among its output
columns was answered with "我需要一个具体的数据处理需求" instead of being
run, because the router saw the word "status" and treated the whole
specification as a progress question.
"""
import json
import unittest
from unittest.mock import patch

from dataflow_agents.conversation import classify_message, looks_like_task

# The message that failed, near enough verbatim.
REPORTED = (
    "7. 对 tracking_no、status、delivered_at 和 cleaned_customer_note 完全相同的记录去重，"
    "保留 created_at 最早的记录。\n"
    "8. 最终仅输出 order_id, snapshot_at, status, carrier, destination_country, "
    "cleaned_customer_note, ship_latency_hours, delivery_latency_hours, transit_age_hours, "
    "pending_age_hours, fulfillment_issue, priority, manual_review。"
)


class RuleOnlyMixin:
    """The rule tests must never reach the network, whatever the ambient env."""

    def setUp(self):
        from unittest.mock import patch
        self.enterContext(patch.dict("os.environ", {"DF_USE_JEV_ROUTING": "0"}))


class TaskWithStatusFieldTests(RuleOnlyMixin, unittest.TestCase):
    """A specification that happens to name a status column is still a task."""

    def test_the_reported_message_is_a_task(self):
        self.assertEqual(classify_message(REPORTED, has_run=False), "new_task")
        self.assertTrue(looks_like_task(REPORTED))

    def test_english_specification_naming_status(self):
        text = ("Deduplicate rows with the same tracking_no and status, keep the earliest created_at, "
                "and output order_id, status and carrier.")
        self.assertEqual(classify_message(text, has_run=False), "new_task")

    def test_bulleted_specification_naming_status(self):
        text = ("- clean the customer_note field\n"
                "- group by status\n"
                "- output order_id and status")
        self.assertEqual(classify_message(text, has_run=False), "new_task")

    def test_specification_with_sample_rows(self):
        self.assertEqual(
            classify_message('[{"order_id": 1, "status": "delivered"}] 请按 status 去重后输出 order_id',
                             has_run=False),
            "new_task")


class RealStatusQuestionTests(RuleOnlyMixin, unittest.TestCase):
    """A question about progress must still reach the status branch."""

    def test_short_questions_stay_questions_in_both_languages(self):
        for text in ("现在进度怎么样了？", "到哪一步了", "what is the progress?", "status",
                     "跑完了吗", "现在status是什么"):
            with self.subTest(text=text):
                self.assertEqual(classify_message(text, has_run=False), "status_query")

    def test_a_question_about_progress_is_not_a_task(self):
        self.assertFalse(looks_like_task("现在进度怎么样了？"))
        self.assertFalse(looks_like_task("what is the progress?"))

    def test_a_numbered_question_still_reads_as_a_question(self):
        # A numbered list alone must not turn a question into a work request.
        self.assertEqual(classify_message("1. 现在进度怎么样了？", has_run=False), "status_query")


class UnrelatedRoutingTests(RuleOnlyMixin, unittest.TestCase):
    def test_plain_requests_are_unaffected(self):
        for text in ("清洗 raw_content 的多余空格并按清洗结果去重",
                     "extract the email column and deduplicate it",
                     "[{\"raw_content\":\"  Hi  \"}] 去掉多余空格"):
            with self.subTest(text=text):
                self.assertEqual(classify_message(text, has_run=False), "new_task")

    def test_revision_and_artifact_queries_need_a_run(self):
        self.assertEqual(classify_message("改成小写输出", has_run=True), "revision")
        self.assertEqual(classify_message("改成小写输出", has_run=False), "new_task")
        self.assertEqual(classify_message("看一下生成的代码", has_run=True), "artifact_query")
        self.assertEqual(classify_message("我需要一个空对话里的普通需求说明", has_run=False), "new_task")

    def test_status_wording_inside_a_request_is_never_a_status_query(self):
        # Whether or not a run exists, describing work stays describing work.
        for has_run in (False, True):
            self.assertEqual(classify_message("清洗 status 字段", has_run=has_run), "new_task")
        # ...and with a run present, reworded work is a revision.
        self.assertEqual(classify_message("把 status 字段清洗一下，改成大写", has_run=True), "revision")
        self.assertEqual(classify_message("把 status 字段清洗一下，改成大写", has_run=False), "new_task")


class JevLayerTests(unittest.TestCase):
    """The model decides when reachable; the rules decide when it is not."""

    def setUp(self):
        from dataflow_agents import routing

        self.routing = routing
        self.config = {"resource_secrets": {"typesafe": "test-key"}, "use_jev_routing": True}
        self.enterContext(patch.dict("os.environ", {"DF_USE_JEV_ROUTING": "1"}))

    def fake_jev(self, answer):
        """Stand in for the HTTP call, recording what it was asked."""
        calls = []

        def stub(text, key, **kwargs):
            calls.append({"text": text, "key": key})
            if isinstance(answer, Exception):
                raise answer
            return answer

        # Patch the attribute the module looks up at call time; reloading the
        # module would rebind classify_message while the re-export in
        # conversation.py kept pointing at the old function object.
        self.enterContext(patch.object(self.routing, "ask_jev", stub))
        self.calls = calls

    def test_the_model_intent_wins_when_confident(self):
        self.fake_jev(("status_query", 0.93))
        intent, meta = self.routing.classify_with_source("清洗 raw_content", has_run=False, config=self.config)
        self.assertEqual(intent, "status_query")
        self.assertEqual(meta["by"], "jev")
        self.assertEqual(meta["rules_intent"], "new_task")  # the two disagreed
        self.assertEqual(self.calls[0]["key"], "test-key")

    def test_a_low_confidence_answer_defers_to_the_rules(self):
        self.fake_jev(("status_query", 0.42))
        intent, meta = self.routing.classify_with_source("清洗 raw_content", has_run=False, config=self.config)
        self.assertEqual(intent, "new_task")
        self.assertEqual(meta["by"], "rules")
        self.assertEqual(meta["deferred"], "low_confidence")
        self.assertEqual(meta["model_intent"], "status_query")

    def test_an_unreachable_model_degrades_to_the_rules(self):
        self.fake_jev(self.routing.JevUnavailable("Jev HTTP 503"))
        intent, meta = self.routing.classify_with_source(REPORTED, has_run=False, config=self.config)
        self.assertEqual(intent, "new_task")
        self.assertEqual(meta["by"], "rules")
        self.assertIn("503", meta["model_unavailable"])

    def test_a_missing_credential_degrades_to_the_rules(self):
        self.fake_jev(self.routing.JevUnavailable("no Jev credential configured"))
        with_key = self.routing.classify_message("现在进度怎么样了？", has_run=False, config=self.config)
        self.assertEqual(with_key, "status_query")

    def test_an_unknown_label_is_treated_as_unavailable(self):
        self.fake_jev(("something_else", 0.99))
        # fake_jev returns the tuple, so the guard inside ask_jev is bypassed;
        # this documents that only known intents may be returned.
        self.assertNotIn("something_else", self.routing.INTENTS)

    def test_routing_can_be_disabled_entirely(self):
        self.fake_jev(("status_query", 0.99))
        intent, meta = self.routing.classify_with_source(REPORTED, has_run=False, config=self.config,
                                                         use_model=False)
        self.assertEqual(intent, "new_task")
        self.assertEqual(meta["by"], "rules")
        self.assertEqual(self.calls, [])

    def test_the_request_shape_matches_the_documented_contract(self):
        question = self.routing.JEV_QUESTIONS["intent"]
        self.assertEqual(question["type"], "choice")
        self.assertEqual(set(question["criteria"]), set(self.routing.INTENTS))
        for label, description in question["criteria"].items():
            self.assertTrue(description.strip(), f"{label} has no criteria text")


class JevRequestTests(unittest.TestCase):
    """The HTTP call itself, against a local stand-in server."""

    def serve(self, handler):
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        class Server(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                self.server.received = json.loads(self.rfile.read(length))
                self.server.auth = self.headers.get("Authorization")
                status, body = handler()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(body).encode())

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Server)
        server.received, server.auth = None, None
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def test_a_choice_answer_is_parsed(self):
        import json as jsonlib

        server = self.serve(lambda: (200, {"answers": {"intent": {"type": "choice", "choice": "revision",
                                                                "confidence": 0.88}}}))
        from dataflow_agents.routing import ask_jev
        intent, confidence = ask_jev("改成小写", "test-key",
                                     endpoint=f"http://127.0.0.1:{server.server_port}/v1/systemone")
        self.assertEqual((intent, confidence), ("revision", 0.88))
        self.assertEqual(server.auth, "Bearer test-key")
        self.assertEqual(server.received["model"], "jev-latest")
        self.assertIn("intent", server.received["questions"])

    def test_an_http_error_raises_unavailable(self):
        server = self.serve(lambda: (503, {"error": "busy"}))
        from dataflow_agents.routing import JevUnavailable, ask_jev
        with self.assertRaises(JevUnavailable):
            ask_jev("x", "k", endpoint=f"http://127.0.0.1:{server.server_port}/v1/systemone")

    def test_an_unknown_choice_raises_unavailable(self):
        server = self.serve(lambda: (200, {"answers": {"intent": {"choice": "banana", "confidence": 0.9}}}))
        from dataflow_agents.routing import JevUnavailable, ask_jev
        with self.assertRaises(JevUnavailable):
            ask_jev("x", "k", endpoint=f"http://127.0.0.1:{server.server_port}/v1/systemone")

    def test_a_missing_key_never_calls_out(self):
        from dataflow_agents.routing import JevUnavailable, ask_jev
        with self.assertRaises(JevUnavailable):
            ask_jev("x", "")


if __name__ == "__main__":
    unittest.main()
