"""Field-only mode validates execution/schema without invoking a model verifier."""
import json
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient
from dataflow_agents.backend import DeterministicBackend
from dataflow_agents.execution import approve
from dataflow_agents.orchestrator import Orchestrator, load_config
from dataflow_agents.verification import verify_fields
from dataflow_agents.web import create_app
from test_execution_continuation import RepairBackend


class FieldCheckTests(unittest.TestCase):
    def verdict(self, output_rows, **overrides):
        runtime = dict(status='passed', compile=True, executed=True, fields=['answer'], rows=len(output_rows))
        runtime.update(overrides)
        return verify_fields({'final_keys': ['answer']}, runtime, output_rows, output_exists=True)

    def test_values_and_semantic_quality_are_not_judged(self):
        for value in ('unrelated answer', None, 42, {'anything': True}):
            self.assertEqual(self.verdict([{'answer': value}])['verdict'], 'pass')

    def test_checks_beyond_the_first_twenty_rows(self):
        rows = [{'answer': 'ok'} for _ in range(25)] + [{'wrong': 'bad field'}]
        verdict = self.verdict(rows)
        self.assertEqual(verdict['verdict'], 'fail')
        self.assertIn('26', verdict['issues'][0])

    def test_extra_fields_and_non_object_rows_fail(self):
        for row in ({'answer': 'ok', 'extra': 1}, ['answer'], 'text'):
            self.assertEqual(self.verdict([row])['verdict'], 'fail')

    def test_runtime_failure_cannot_be_overridden_by_correct_fields(self):
        self.assertEqual(self.verdict([{'answer': 'ok'}], status='failed')['verdict'], 'fail')
        self.assertEqual(self.verdict([{'answer': 'ok'}], executed=False)['verdict'], 'fail')
        self.assertEqual(self.verdict([{'answer': 'ok'}], compile=False)['verdict'], 'fail')

    def test_empty_output_requires_successful_schema_evidence(self):
        self.assertEqual(self.verdict([])['verdict'], 'pass')
        self.assertEqual(self.verdict([], fields=[])['verdict'], 'fail')

    def test_missing_output_and_wrong_row_count_fail(self):
        runtime = dict(status='passed', compile=True, executed=True, fields=['answer'], rows=0)
        verdict = verify_fields({'final_keys': ['answer']}, runtime, [], output_exists=False)
        self.assertEqual(verdict['verdict'], 'fail')
        self.assertEqual(self.verdict([{'answer': 'ok'}], rows=2)['verdict'], 'fail')


class NoVerifierBackend(DeterministicBackend):
    def ask(self, role, prompt, **kwargs):
        if role == 'verifier':
            raise AssertionError('Field-only mode must not ask a model verifier')
        return super().ask(role, prompt, **kwargs)


class FieldModeWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'run-fields'
        self.cfg = load_config(backend='offline', auto_execute=True, verification_mode='fields',
                               max_repairs=0, runs_root=self.tmp.name)
        self.cfg.update(resources={}, resource_secrets={})

    def test_real_execution_passes_without_model_verifier_and_api_reports_mode(self):
        engine = Orchestrator(config=self.cfg, backend=NoVerifierBackend())
        result = engine.run('清洗空格并去重，输出 cleaned_content', run_dir=self.root)
        self.assertEqual(result['state'], 'VERIFIED', result)
        verification = json.loads((self.root / 'verification.json').read_text())
        self.assertEqual(verification['mode'], 'fields')
        self.assertEqual(verification['verdict'], 'pass')
        jobs = json.loads((self.root / 'jobs.json').read_text())
        self.assertNotIn('verifier', {job['role'] for job in jobs})
        events = [json.loads(x) for x in (self.root / 'events.jsonl').read_text().splitlines()]
        self.assertFalse(any(e.get('skill') == 'verification-evidence' for e in events))
        app = create_app(self.cfg)
        self.addCleanup(app.state.executor.shutdown)
        with TestClient(app) as client:
            self.assertEqual(client.get('/api/v1/health').json()['verification_mode'], 'fields')
            self.assertEqual(client.get('/api/v1/runs/run-fields').json()['verification_mode'], 'fields')

    def test_old_semantic_failure_can_continue_in_field_mode(self):
        backend = RepairBackend()
        cfg = dict(self.cfg, verification_mode='semantic', max_repairs=1)
        engine = Orchestrator(config=cfg, backend=backend)
        self.assertEqual(engine.run('Count A', run_dir=self.root)['state'], 'APPROVAL_REQUIRED')
        approve(self.root)
        self.assertEqual(engine.resume(self.root)['state'], 'APPROVAL_REQUIRED')
        self.assertEqual(json.loads((self.root / 'verification.json').read_text())['verdict'], 'fail')
        approve(self.root)
        result = Orchestrator(config=self.cfg, backend=NoVerifierBackend()).resume(self.root)
        self.assertEqual(result['state'], 'VERIFIED', result)
        self.assertEqual(json.loads((self.root / 'verification.json').read_text())['mode'], 'fields')
