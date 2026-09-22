import fcntl
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from dataflow_agents.orchestrator import load_config
from dataflow_agents.team import TeamStore
from dataflow_agents.web import create_app, _events, _remove_run_tree


class RunDeletionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'run-delete-test'
        self.app = create_app(load_config(backend='offline', runs_root=self.tmp.name))
        self.addCleanup(self.app.state.executor.shutdown)
        self.client = self.enterContext(TestClient(self.app))
        self.url = '/api/v1/runs/run-delete-test'
        TeamStore(self.root).checkpoint('READY')

    def test_repeated_concurrent_deletes_and_input_cleanup(self):
        inputs = Path(self.tmp.name) / '.api-inputs'
        inputs.mkdir()
        input_file = inputs / 'run-delete-test.jsonl'
        input_file.write_text('{}\n')
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: self.client.delete(self.url), range(2)))
        self.assertEqual([r.status_code for r in results], [200, 200])
        self.assertFalse(self.root.exists())
        self.assertFalse(input_file.exists())
        self.assertEqual(_events(self.root), [])
        self.assertFalse(self.root.exists())

    def test_disappearing_child_during_rmtree(self):
        import os
        target = self.root / 'vanishing-cache'
        target.write_text('temporary')
        original = os.unlink
        def unlink(path, *args, **kwargs):
            if Path(path).name == target.name:
                original(path, *args, **kwargs)
            return original(path, *args, **kwargs)
        with patch('os.unlink', side_effect=unlink):
            response = self.client.delete(self.url)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(self.root.exists())

    def test_active_run_and_terminal_state_with_held_lock_are_protected(self):
        # A genuinely active run is one whose worker holds the leader lock;
        # holding it is what makes the run busy.
        TeamStore(self.root).checkpoint('PLANNING')
        with (self.root / '.leader.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(self.client.delete(self.url).status_code, 409)
        TeamStore(self.root).checkpoint('READY')
        with (self.root / '.leader.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(self.client.delete(self.url).status_code, 409)
        self.assertTrue(self.root.exists())
        self.assertEqual(self.client.delete(self.url).status_code, 200)

    def test_a_run_wedged_by_a_restart_is_still_deletable(self):
        """The state alone must not make a run undeletable.

        A server restart leaves a run's status mid-flight with no process
        holding its lock. Refusing on the recorded state made such a run
        permanent: it was neither running nor removable.
        """
        TeamStore(self.root).checkpoint('INTEGRATING')
        TeamStore(self.root).event('agent.started', 'pipeline_integrator')
        # Nothing holds the lock, exactly as after a restart.
        self.assertEqual(self.client.delete(self.url).status_code, 200)
        self.assertFalse(self.root.exists())

    def test_permission_failure_is_not_silent_success(self):
        with patch('dataflow_agents.web._remove_run_tree', side_effect=PermissionError('denied')):
            response = self.client.delete(self.url)
        self.assertEqual(response.status_code, 409)
        self.assertIn('permissions', response.json()['detail'])
        self.assertTrue(self.root.exists())

    def test_symlink_target_is_preserved(self):
        outside = Path(self.tmp.name) / 'outside'
        outside.mkdir()
        file = outside / 'keep.txt'
        file.write_text('keep')
        (self.root / 'cache-link').symlink_to(outside, target_is_directory=True)
        self.assertEqual(self.client.delete(self.url).status_code, 200)
        self.assertEqual(file.read_text(), 'keep')
