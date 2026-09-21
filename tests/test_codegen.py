"""Generated pipelines must read like the pipelines DataFlow ships."""
import ast
import json
import tempfile
import unittest
from pathlib import Path

from dataflow_agents.codegen import (RUNNER_FILENAME, literal, pipeline_class_name,
                                     render_dataflow_pipeline, write_pipeline_sources)
from dataflow_agents.pipeline_runner import clear_stage_files, explain_empty_stage, stage_rows


def step(**overrides):
    base = {'step_id': 'step_1', 'operator': 'RemoveExtraSpacesRefiner',
            'import_path': 'dataflow.operators.general_text',
            'module': 'dataflow.operators.general_text.refine.remove_extra_spaces_refiner',
            'proposal': None, 'init_args': {}, 'prepare_fields': {},
            'run_args': {'input_key': 'raw_content'}}
    base.update(overrides)
    return base


def spec(steps, **overrides):
    base = {'steps': steps, 'initial_keys': ['raw_content'], 'final_keys': ['raw_content'],
            'resources': {}, 'servings': {}}
    base.update(overrides)
    return base


class CodegenTests(unittest.TestCase):
    def test_source_is_plain_dataflow_without_workbench_runtime(self):
        source = render_dataflow_pipeline(spec([step()]), request='清理空格')
        tree = ast.parse(source)
        self.assertIn('from dataflow.operators.general_text import RemoveExtraSpacesRefiner', source)
        # The defining module is an implementation detail; DataFlow examples
        # always import from the operator package.
        self.assertNotIn('remove_extra_spaces_refiner import', source)
        # No embedded spec, no generic interpreter, no argparse harness.
        for leaked in ('SPEC = ', 'OPERATOR_REGISTRY', 'argparse', 'importlib', 'RESOURCE_CACHE'):
            self.assertNotIn(leaked, source)
        classes = [node.name for node in tree.body if isinstance(node, ast.ClassDef)]
        self.assertEqual(classes, ['GeneralText_CPUPipeline'])
        self.assertIn('    pipeline = GeneralText_CPUPipeline()\n    pipeline.compile()\n    pipeline.forward()', source)

    def test_operators_are_named_attributes_called_in_order(self):
        steps = [step(prepare_fields={'cleaned': 'raw_content'}),
                 step(step_id='step_2', operator='HashDeduplicateFilter',
                      init_args={'hash_func': 'md5'},
                      run_args={'input_key': 'cleaned', 'output_key': 'label'})]
        source = render_dataflow_pipeline(spec(steps))
        self.assertIn('self.copy_cleaned_step1 = CopyFieldRefiner()', source)
        self.assertIn('self.remove_extra_spaces_refiner_step1 = RemoveExtraSpacesRefiner()', source)
        self.assertIn('self.hash_deduplicate_filter_step2 = HashDeduplicateFilter(\n            hash_func="md5",\n        )', source)
        called = [line.strip() for line in source.splitlines() if line.strip().startswith('self.') and '.run(' in line]
        self.assertEqual(called, ['self.copy_cleaned_step1.run(',
                                  'self.remove_extra_spaces_refiner_step1.run(',
                                  'self.hash_deduplicate_filter_step2.run('])
        self.assertEqual(source.count('storage=self.storage.step(),'), 3)

    def test_serving_is_constructed_inline_and_referenced_by_attribute(self):
        resources = {'llm_default': {'type': 'api_llm', 'args': {
            'api_url': 'https://api.example.com/v1', 'model_name': 'gpt-4o',
            'key_name_of_api_key': 'DF_PIPELINE_LLM', 'max_workers': 4}}}
        generator = step(step_id='step_1', operator='ReasoningAnswerGenerator',
                         import_path='dataflow.operators.reasoning',
                         init_args={'llm_serving': {'$resource': 'llm_default'}},
                         run_args={'input_key': 'question', 'output_key': 'answer'})
        source = render_dataflow_pipeline(spec([generator], resources=resources, servings=resources))
        self.assertIn('from dataflow.serving import APILLMServing_request', source)
        self.assertIn('api_url="https://api.example.com/v1/chat/completions"', source)
        self.assertIn('self.llm_default = APILLMServing_request(', source)
        self.assertIn('llm_serving=self.llm_default,', source)
        self.assertEqual(pipeline_class_name(spec([generator], servings=resources)), 'Reasoning_APIPipeline')

    def test_unresolvable_references_fail_loudly(self):
        dangling = step(init_args={'llm_serving': {'$resource': 'missing'}})
        with self.assertRaisesRegex(ValueError, 'undeclared serving resource'):
            render_dataflow_pipeline(spec([dangling]))
        nameless = step()
        del nameless['import_path'], nameless['module']
        with self.assertRaisesRegex(ValueError, 'no operator module'):
            render_dataflow_pipeline(spec([nameless]))

    def test_generated_operator_is_imported_from_the_run_package(self):
        proposal = {'name': 'CountUpperA', 'source': 'class CountUpperA: pass\n', 'tests': []}
        generated = step(operator='CountUpperA', proposal=proposal, import_path='custom.CountUpperA',
                         source_file='custom/CountUpperA.py', module='custom.CountUpperA')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_pipeline_sources(root, spec([generated]), 'count letters')
            source = (root / 'pipeline.py').read_text()
            self.assertIn('from custom.CountUpperA import CountUpperA', source)
            self.assertTrue((root / 'custom/__init__.py').exists())
            self.assertEqual((root / 'custom/CountUpperA.py').read_text(), proposal['source'])
            runner = (root / RUNNER_FILENAME).read_text()
            self.assertIn('def guard_servings', runner)
            ast.parse(runner)

    def test_values_render_as_reviewable_python_literals(self):
        self.assertEqual(literal('a"b'), '"a\\"b"')
        self.assertEqual(literal([1, 2]), '[1, 2]')
        self.assertEqual(literal({'a': None, 'b': True}), '{"a": None, "b": True}')
        nested = {'keys': ['x'] * 30}
        self.assertIn('\n', literal(nested))
        self.assertEqual(ast.literal_eval(literal(nested)), json.loads(json.dumps(nested)))


class RuntimeDiagnosticsTests(unittest.TestCase):
    """An emptied stage must be reported as such, not as a missing column."""

    def counts(self, directory, sizes):
        for index, rows in enumerate(sizes, start=1):
            path = Path(directory) / f"dataflow_cache_step_step{index}.jsonl"
            path.write_text("".join(json.dumps({"a": i}) + "\n" for i in range(rows)), encoding="utf-8")
        return stage_rows(directory)

    def test_stage_rows_follow_execution_order(self):
        with tempfile.TemporaryDirectory() as directory:
            # Written out of order, and with a file the pipeline did not produce.
            (Path(directory) / "unrelated.jsonl").write_text("{}\n", encoding="utf-8")
            rows = self.counts(directory, [3, 0, 2])
            self.assertEqual([item["step"] for item in rows], [1, 2, 3])
            self.assertEqual([item["rows"] for item in rows], [3, 0, 2])

    def test_empty_stage_names_the_operator_that_dropped_every_row(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = self.counts(directory, [0, 0])
            message = explain_empty_stage(rows, ['reasoning_question_filter_step1', 'answer_step2'])
            self.assertIn('Step 1 (reasoning_question_filter_step1) wrote 0 rows', message)
            self.assertIn('filtered out', message)
        self.assertIsNone(explain_empty_stage([{'step': 1, 'rows': 4}], ['keep_step1']))
        # An unnamed step still produces a usable message.
        self.assertIn('step 2', explain_empty_stage([{'step': 2, 'rows': 0}], []))


class StaleStageTests(unittest.TestCase):
    """A re-run must not leave the previous attempt's rows behind."""

    def cache(self, stage_rows_written):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        for index, rows in enumerate(stage_rows_written, start=1):
            path = Path(directory.name) / f"dataflow_cache_step_step{index}.jsonl"
            path.write_text("".join("{}" + "\n" for _ in range(rows)), encoding="utf-8")
        # A fixture directory from an earlier run, which must survive.
        fixture = Path(directory.name) / "test_step_1_0"
        fixture.mkdir()
        (fixture / "input.jsonl").write_text("{}\n", encoding="utf-8")
        return directory.name

    def test_previous_stage_files_are_removed_before_executing(self):
        cache = self.cache([2, 2, 2])
        clear_stage_files(cache)
        self.assertEqual(list(Path(cache).glob("*_step*.jsonl")), [])
        self.assertEqual(stage_rows(cache), [])

    def test_fixture_directories_and_unrelated_files_survive(self):
        cache = self.cache([1])
        (Path(cache) / "notes.txt").write_text("keep", encoding="utf-8")
        clear_stage_files(cache)
        self.assertTrue((Path(cache) / "test_step_1_0" / "input.jsonl").exists())
        self.assertTrue((Path(cache) / "notes.txt").exists())

    def test_a_missing_cache_directory_is_not_an_error(self):
        with tempfile.TemporaryDirectory() as directory:
            clear_stage_files(Path(directory) / "does-not-exist")

    def test_the_failure_that_prompted_this_reports_only_the_stage_it_reached(self):
        """Failed at step 1 with two stale files present: only step 1 is real."""
        cache = self.cache([2, 2])
        # What the run writes before failing: step 1 emptied, step 2 never
        # reached, so with the fix only step 1 exists.
        (Path(cache) / "dataflow_cache_step_step1.jsonl").write_text("", encoding="utf-8")
        clear_stage_files(cache)
        (Path(cache) / "dataflow_cache_step_step1.jsonl").write_text("", encoding="utf-8")
        rows = stage_rows(cache)
        self.assertEqual([(item["step"], item["rows"]) for item in rows], [(1, 0)])
        message = explain_empty_stage(rows, ["reasoning_question_filter_step1", "answer_step2"])
        self.assertIn("Step 1", message)
