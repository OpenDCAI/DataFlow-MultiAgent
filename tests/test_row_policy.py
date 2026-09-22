import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dataflow_agents.backend import DeterministicBackend
from dataflow_agents.execution import approve
from dataflow_agents.orchestrator import Orchestrator, load_config
from dataflow_agents.row_policy import preserve_all_rows, row_filter_errors, row_loss_error
from test_orchestrator import CustomBackend

FIXTURE = Path(__file__).parent / 'fixtures/order_no_filter.json'


class RowPolicyTests(unittest.TestCase):
    def test_explicit_chinese_and_english_row_constraints(self):
        for text in ('保留所有原始记录。', '不删除任何记录', '不要写过滤算子',
                     '禁止过滤数据行', 'Keep all input rows', 'without row filtering',
                     'Do not use filtering operators'):
            with self.subTest(text=text):
                self.assertTrue(preserve_all_rows(text))
        self.assertTrue(preserve_all_rows('', {'allow_filtering': False}))

    def test_preserving_columns_and_conditional_exceptions_are_not_global_row_bans(self):
        for text in ('不得删除原始字段', '保留全部原始输入字段',
                     '不得删除 quality_label 为 reject 的记录；对其他记录去重',
                     '过滤不能求解的数学问题', 'remove HTML tags', 'valid_order invalid-value'):
            self.assertFalse(preserve_all_rows(text), text)

    def test_explicit_filter_request_remains_allowed(self):
        bindings = [{'step_id': 'step1', 'operator': 'ReasoningQuestionFilter'}]
        self.assertEqual(row_filter_errors(bindings, [], False), [])
        self.assertTrue(row_filter_errors(bindings, [], True))
        self.assertEqual(row_filter_errors([{'step_id': 'step1', 'operator': 'NumericCoercionRefiner'}], [], True), [])

    def test_row_loss_detected_without_banning_expansion(self):
        self.assertIsNotNone(row_loss_error(True, 6, 0))
        self.assertIsNone(row_loss_error(True, 6, 6))
        self.assertIsNone(row_loss_error(True, 6, 12))
        self.assertIsNone(row_loss_error(False, 6, 0))


class OrderBackend(DeterministicBackend):
    def __init__(self, insert_filter=False):
        self.fixture = json.loads(FIXTURE.read_text())
        self.insert_filter = insert_filter
        self.saw_constraints = []

    def ask(self, role, prompt, **kwargs):
        self.saw_constraints.append(prompt.get('constraints', {}))
        if role == 'planner':
            return copy.deepcopy(self.fixture['plan'])
        if role == 'operator_specialist':
            return copy.deepcopy(next(b for b in self.fixture['integrated']['bindings']
                                      if b['step_id'] == prompt['step']['step_id']))
        if role == 'pipeline_integrator':
            result = copy.deepcopy(self.fixture['integrated'])
            if self.insert_filter:
                b = result['bindings'][1]
                b.update(operator='ReasoningQuestionFilter', proposal=None,
                         init_args={'llm_serving': {'$resource': 'llm_default'}},
                         run_args={'input_key': 'amount'}, prepare_fields={})
            return result
        raise AssertionError('No model verifier or external calls expected: ' + role)


class RowPreservingWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'run-orders'
        self.cfg = load_config(backend='offline', auto_execute=True, verification_mode='fields',
                               max_repairs=0, runs_root=self.tmp.name)
        self.cfg.update(resources={}, resource_secrets={})
        self.rows = [{'order_id': f'order-{i}', 'amount': amount, 'status': status}
                     for i, (amount, status) in enumerate([
                         ('12', 'paid'), ('10.5', 'PAYED'), ('0', 'cancelled'),
                         ('-1', 'completed'), ('abc', 'unknown'), (None, 'cancel')])]
        self.input = Path(self.tmp.name) / 'orders.jsonl'
        self.input.write_text(''.join(json.dumps(r) + '\n' for r in self.rows))
        self.request = ('标准化 status，将 amount 转为数值，生成 valid_order/other_order 的 order_label；'
                        '保留所有原始记录，不要写过滤算子。输出 order_id、amount、status、order_label。')

    def test_reported_order_pipeline_retains_custom_operators_and_all_six_rows(self):
        backend = OrderBackend()
        engine = Orchestrator(config=self.cfg, backend=backend)
        result = engine.run(self.request, run_dir=self.root, input_file=self.input)
        self.assertEqual(result['state'], 'APPROVAL_REQUIRED', result)
        spec = json.loads((self.root / 'pipeline-spec.json').read_text())
        self.assertEqual([s['operator'] for s in spec['steps']],
                         ['EcommerceStatusNormalizer', 'NumericCoercionRefiner', 'GenerateOrderLabelOperator'])
        self.assertEqual(spec['resources'], {})
        self.assertTrue(all(c.get('preserve_all_rows') for c in backend.saw_constraints))
        approve(self.root)
        result = engine.resume(self.root)
        self.assertEqual(result['state'], 'VERIFIED', result)
        output = [json.loads(line) for line in (self.root / 'output.jsonl').read_text().splitlines()]
        self.assertEqual(len(output), 6)
        self.assertEqual([r['order_id'] for r in output], [r['order_id'] for r in self.rows])
        self.assertEqual([r['order_label'] for r in output], ['valid_order', 'valid_order'] + ['other_order'] * 4)

    def test_model_selected_filter_is_rejected_before_execution(self):
        engine = Orchestrator(config=self.cfg, backend=OrderBackend(insert_filter=True))
        with patch('dataflow_agents.orchestrator.execute', side_effect=AssertionError('Must not run filters')) as execute:
            result = engine.run(self.request, run_dir=self.root, input_file=self.input)
        self.assertEqual(result['state'], 'BLOCKED', result)
        self.assertIn('ReasoningQuestionFilter', result['error'])
        execute.assert_not_called()

    def test_older_paused_filter_is_also_rejected_before_execution(self):
        engine = Orchestrator(config=self.cfg, backend=OrderBackend(insert_filter=True))
        # Simulate a run generated before explicit row preservation was enforced.
        with patch('dataflow_agents.orchestrator.row_filter_errors', return_value=[]):
            result = engine.run(self.request, run_dir=self.root, input_file=self.input)
        self.assertIn(result['state'], {'APPROVAL_REQUIRED', 'RESOURCE_REQUIRED'})
        with patch('dataflow_agents.orchestrator.execute', side_effect=AssertionError('Must not run filters')) as execute:
            result = engine.resume(self.root)
        self.assertEqual(result['state'], 'BLOCKED', result)
        execute.assert_not_called()

    def test_custom_operator_cannot_hide_row_loss_under_a_refiner_name(self):
        class DroppingBackend(CustomBackend):
            def ask(self, role, prompt, **kwargs):
                result = super().ask(role, prompt, **kwargs)
                if role == 'operator_specialist':
                    result['proposal']['source'] = result['proposal']['source'].replace(
                        "df = storage.read('dataframe').copy()", "df = storage.read('dataframe').copy().iloc[:1]")
                    result['proposal']['tests'] = [{'input': [{'raw_content': 'A'}],
                                                    'expected': [{'raw_content': 'A', 'a_count': 1}]}]
                return result
        engine = Orchestrator(config=self.cfg, backend=DroppingBackend())
        result = engine.run('Count A; keep all input rows', run_dir=self.root)
        self.assertEqual(result['state'], 'APPROVAL_REQUIRED', result)
        approve(self.root)
        result = engine.resume(self.root)
        self.assertEqual(result['state'], 'BLOCKED', result)
        self.assertIn('保留全部记录', result['error'])
        self.assertFalse((self.root / 'output.jsonl').exists())
