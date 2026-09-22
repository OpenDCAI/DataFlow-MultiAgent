"""Continue the reviewed repair, rather than regenerating round zero on resume."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

from test_orchestrator import CustomBackend
from dataflow_agents.execution import approve, manifest
from dataflow_agents.orchestrator import Orchestrator, load_config
from dataflow_agents.team import TeamStore, write_json


class RepairBackend(CustomBackend):
    def __init__(self, reject_repaired=False):
        self.calls = []
        self.reject_repaired = reject_repaired

    def ask(self, role, prompt, **kwargs):
        self.calls.append((role, prompt.get('job_id')))
        if role == 'verifier':
            repaired = '# reviewed repair' in prompt['pipeline']['steps'][0]['proposal']['source']
            passed = repaired and not self.reject_repaired
            return {'verdict': 'pass' if passed else 'fail',
                    'reason': 'Checked repaired version' if passed else 'Needs semantic repair',
                    'issues': [] if passed else ['Revise the generated operator']}
        result = copy.deepcopy(super().ask(role, prompt, **kwargs))
        if role == 'pipeline_integrator' and prompt.get('validation_feedback'):
            result['bindings'][0]['proposal']['source'] += '\n# reviewed repair\n'
        return result


class ExecutionContinuationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'run-continuation'
        self.cfg = load_config(backend='offline', auto_execute=True, max_repairs=1,
                               runs_root=self.tmp.name, verification_mode='semantic')
        self.cfg.update(resources={}, resource_secrets={})
        self.backend = RepairBackend()
        self.engine = Orchestrator(config=self.cfg, backend=self.backend)

    def repair_pause(self):
        self.assertEqual(self.engine.run('Count A', run_dir=self.root)['state'], 'APPROVAL_REQUIRED')
        approve(self.root)
        self.assertEqual(self.engine.resume(self.root)['state'], 'APPROVAL_REQUIRED')
        self.assertIn('# reviewed repair', (self.root / 'custom/CountUpperA.py').read_text())
        checkpoint = json.loads((self.root / 'execution-continuation.json').read_text())
        self.assertEqual(checkpoint['repair'], 1)
        return manifest(self.root)

    def test_approved_repair_survives_restart_and_reaches_verifier_once(self):
        reviewed = self.repair_pause()
        approve(self.root)
        self.backend.calls.clear()
        restarted = Orchestrator(config=self.cfg, backend=self.backend)
        result = restarted.resume(self.root)
        self.assertEqual(result['state'], 'VERIFIED', result)
        self.assertEqual(self.backend.calls, [('verifier', 'verifier-1')])
        self.assertEqual(manifest(self.root), reviewed)
        events = [json.loads(line) for line in (self.root / 'events.jsonl').read_text().splitlines()]
        verdicts = [event['verdict'] for event in events if event['event'] == 'pipeline.verification']
        self.assertEqual(verdicts, ['fail', 'pass'])
        self.assertTrue(any(e.get('state') == 'REPAIRING' for e in events))
        self.assertTrue((self.root / 'output.jsonl').exists())

    def test_unapproved_resume_preserves_version_and_makes_no_model_calls(self):
        reviewed = self.repair_pause()
        self.backend.calls.clear()
        for _ in range(2):
            result = self.engine.resume(self.root)
            self.assertEqual(result['state'], 'APPROVAL_REQUIRED', result)
            self.assertEqual(manifest(self.root), reviewed)
        self.assertEqual(self.backend.calls, [])

    def test_legacy_paused_repair_works_even_after_web_sets_queued(self):
        reviewed = self.repair_pause()
        (self.root / 'execution-continuation.json').unlink()
        approve(self.root)
        TeamStore(self.root).checkpoint('QUEUED')
        self.backend.calls.clear()
        result = Orchestrator(config=self.cfg, backend=self.backend).resume(self.root)
        self.assertEqual(result['state'], 'VERIFIED', result)
        self.assertEqual(self.backend.calls, [('verifier', 'verifier-1')])
        self.assertEqual(manifest(self.root), reviewed)

    def test_repair_budget_is_not_reset_by_approval_or_restart(self):
        self.backend.reject_repaired = True
        self.repair_pause()
        approve(self.root)
        self.backend.calls.clear()
        # A saved run's remaining budget must not be reset by a fresh process.
        changed_config = dict(self.cfg, max_repairs=10)
        result = Orchestrator(config=changed_config, backend=self.backend).resume(self.root)
        self.assertEqual(result['state'], 'BLOCKED', result)
        self.assertEqual(self.backend.calls, [('verifier', 'verifier-1')])
        self.assertFalse((self.root / 'output.jsonl').exists())

    def test_changed_code_requires_new_approval_and_is_not_overwritten(self):
        self.repair_pause()
        approve(self.root)
        path = self.root / 'custom/CountUpperA.py'
        path.write_text(path.read_text() + '\n# user revision\n')
        changed = manifest(self.root)
        self.backend.calls.clear()
        result = self.engine.resume(self.root)
        self.assertEqual(result['state'], 'APPROVAL_REQUIRED', result)
        self.assertEqual(manifest(self.root), changed)
        self.assertEqual(self.backend.calls, [])

    def test_resource_update_continues_current_repair_without_replanning(self):
        self.repair_pause()
        spec = json.loads((self.root / 'pipeline-spec.json').read_text())
        resource = {'type': 'api_llm', 'args': {
            'api_url': 'https://configure-resource.example.invalid/v1',
            'key_name_of_api_key': 'DF_PIPELINE_TEST', 'model_name': 'test'}}
        spec['resources'] = {'llm_default': resource}
        spec['servings'] = dict(spec['resources'])
        write_json(self.root / 'pipeline-spec.json', spec)
        result = self.engine.resume(self.root)
        self.assertEqual(result['state'], 'RESOURCE_REQUIRED', result)
        configured = copy.deepcopy(resource)
        configured['args']['api_url'] = 'https://configured.example.invalid/v1'
        cfg = dict(self.cfg, resources={'llm_default': configured}, resource_secrets={'llm_default': 'test-only'})
        self.backend.calls.clear()
        # Stops at approval before any real external API request.
        result = Orchestrator(config=cfg, backend=self.backend).resume(self.root)
        self.assertEqual(result['state'], 'APPROVAL_REQUIRED', result)
        self.assertEqual(self.backend.calls, [])
        updated = json.loads((self.root / 'pipeline-spec.json').read_text())
        self.assertEqual(updated['resources']['llm_default'], configured)
        self.assertIn('# reviewed repair', (self.root / 'custom/CountUpperA.py').read_text())
