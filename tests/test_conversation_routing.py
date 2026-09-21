"""Routing a message must not be decided by a field name inside it.

Reported from the field: a request that listed `status` among its output
columns was answered with "我需要一个具体的数据处理需求" instead of being
run, because the router saw the word "status" and treated the whole
specification as a progress question.
"""
import unittest

from dataflow_agents.conversation import classify_message, looks_like_task

# The message that failed, near enough verbatim.
REPORTED = (
    "7. 对 tracking_no、status、delivered_at 和 cleaned_customer_note 完全相同的记录去重，"
    "保留 created_at 最早的记录。\n"
    "8. 最终仅输出 order_id, snapshot_at, status, carrier, destination_country, "
    "cleaned_customer_note, ship_latency_hours, delivery_latency_hours, transit_age_hours, "
    "pending_age_hours, fulfillment_issue, priority, manual_review。"
)


class TaskWithStatusFieldTests(unittest.TestCase):
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


class RealStatusQuestionTests(unittest.TestCase):
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


class UnrelatedRoutingTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
