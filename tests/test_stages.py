"""Every operator's materialized output must be reviewable, not just the last."""
import json
import tempfile
import unittest
from pathlib import Path

from dataflow_agents.web import _stage_snapshots


def jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


class StageSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "run-stages"
        (self.root / "cache").mkdir(parents=True)
        (self.root / "pipeline-spec.json").write_text(json.dumps({"steps": [
            {"operator": "RemoveExtraSpacesRefiner", "prepare_fields": {"clean": "raw_content"}},
            {"operator": "HashDeduplicateFilter", "prepare_fields": {}},
        ]}), encoding="utf-8")

    def stages(self, prefix="dataflow_cache_step"):
        jsonl(self.root / f"cache/{prefix}_step1.jsonl", [{"raw_content": "a", "clean": "a"}] * 3)
        jsonl(self.root / f"cache/{prefix}_step2.jsonl", [{"raw_content": "a", "clean": "a"}] * 3)
        jsonl(self.root / f"cache/{prefix}_step3.jsonl", [{"raw_content": "a", "clean": "a", "label": 1}] * 2)
        return _stage_snapshots(self.root)

    def test_each_call_is_a_stage_named_after_what_produced_it(self):
        stages = self.stages()
        self.assertEqual([stage["name"] for stage in stages],
                         ["Copy raw_content -> clean", "RemoveExtraSpacesRefiner", "HashDeduplicateFilter"])
        self.assertEqual([stage["row_count"] for stage in stages], [3, 3, 2])
        self.assertEqual([stage["index"] for stage in stages], [0, 1, 2])
        self.assertEqual(stages[-1]["fields"], ["raw_content", "clean", "label"])

    def test_the_prefix_the_pipeline_chose_is_not_assumed(self):
        # Pipelines generated before the native-style rewrite used "pipeline".
        self.assertEqual(len(self.stages(prefix="pipeline")), 3)

    def test_final_projection_is_shown_next_to_the_stages_not_instead(self):
        jsonl(self.root / "output.jsonl", [{"clean": "a"}] * 2)
        stages = self.stages()
        self.assertEqual(len(stages), 4)
        self.assertEqual(stages[-1]["stage_id"], "final")
        self.assertEqual(stages[-1]["fields"], ["clean"])
        # Stages survive even when nothing was delivered.
        (self.root / "output.jsonl").unlink()
        self.assertEqual(len(self.stages()), 3)

    def test_an_emptied_stage_is_still_listed(self):
        jsonl(self.root / "cache/dataflow_cache_step_step1.jsonl", [])
        stages = _stage_snapshots(self.root)
        self.assertEqual(stages[0]["row_count"], 0)
        self.assertEqual(stages[0]["rows"], [])

    def test_ordering_is_numeric_and_fixture_runs_are_excluded(self):
        for index in (1, 2, 10):
            jsonl(self.root / f"cache/dataflow_cache_step_step{index}.jsonl", [{"raw_content": index}])
        jsonl(self.root / "cache/test_step_1_0/dataflow_cache_step_step1.jsonl", [{"fixture": True}])
        stages = _stage_snapshots(self.root)
        self.assertEqual([stage["stage_id"] for stage in stages], ["stage-01", "stage-02", "stage-10"])
        self.assertNotIn("fixture", json.dumps(stages))

    def test_row_preview_is_capped_while_the_count_stays_true(self):
        jsonl(self.root / "cache/dataflow_cache_step_step1.jsonl", [{"raw_content": n} for n in range(50)])
        stage = _stage_snapshots(self.root, limit=5)[0]
        self.assertEqual(len(stage["rows"]), 5)
        self.assertEqual(stage["row_count"], 50)
