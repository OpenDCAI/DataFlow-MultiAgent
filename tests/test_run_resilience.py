"""A deleted diagnostic and one corrupt run must not erase the history view."""
import json
import shutil
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
from dataflow_agents.orchestrator import load_config
from dataflow_agents.team import TeamStore, write_json
from dataflow_agents.web import create_app, _run_updated


class RunResilienceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.app = create_app(load_config(backend='offline', runs_root=self.tmp.name))
        self.addCleanup(self.app.state.executor.shutdown)
        self.client = self.enterContext(TestClient(self.app))
        self.good = self.root / 'run-healthy'
        TeamStore(self.good).checkpoint('VERIFIED')
        write_json(self.good / 'request.json', {'request': '保留这条成功记录'})

    def test_empty_or_corrupt_database_does_not_hide_healthy_history(self):
        for name, content in [('run-empty', b''), ('run-corrupt', b'not a sqlite database')]:
            root = self.root / name
            root.mkdir()
            (root / 'team.sqlite').write_bytes(content)
        response = self.client.get('/api/v1/runs')
        self.assertEqual(response.status_code, 200)
        records = {item['run_id']: item for item in response.json()}
        self.assertEqual(records['run-healthy']['state'], 'VERIFIED')
        for name in ('run-empty', 'run-corrupt'):
            self.assertTrue(records[name]['record_error'])
            self.assertEqual(records[name]['state'], 'BLOCKED')
        self.assertEqual((self.root / 'run-empty/team.sqlite').stat().st_size, 0)
        self.assertEqual((self.root / 'run-corrupt/team.sqlite').read_bytes(), b'not a sqlite database')

    def test_invalid_metadata_and_mixed_timestamps_cannot_break_sorting(self):
        for index, status in enumerate(([1], {'state': 'READY', 'updated': None},
                                        {'state': 'READY', 'updated': 'yesterday'},
                                        {'state': 'READY', 'updated': float('nan')})):
            root = self.root / f'run-metadata-{index}'
            write_json(root / 'status.json', status)
        response = self.client.get('/api/v1/runs')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()), 5)
        self.assertTrue(next(r for r in response.json() if r['run_id'] == 'run-metadata-0')['record_error'])

    def test_deleted_directory_during_listing_does_not_break_other_records(self):
        disappearing = self.root / 'run-disappearing'
        TeamStore(disappearing).checkpoint('READY')
        def sort_key(root):
            if root == disappearing.resolve():
                shutil.rmtree(root)
            return _run_updated(root)
        with patch('dataflow_agents.web._run_updated', side_effect=sort_key):
            response = self.client.get('/api/v1/runs')
        self.assertEqual(response.status_code, 200)
        self.assertEqual([r['run_id'] for r in response.json()], ['run-healthy'])
        self.assertFalse(disappearing.exists())

    def test_late_database_writer_cannot_recreate_empty_database(self):
        store = TeamStore(self.root / 'run-late-writer')
        shutil.rmtree(store.root)
        store.root.mkdir()  # Another stale writer has recreated the directory.
        with self.assertRaises(sqlite3.OperationalError):
            store.event('agent.failed')
        self.assertFalse(store.db_path.exists())

    def queued_diagnosis(self):
        """Exercise real HTTP creation and reporting without calling an external model."""
        queued = []
        conversation = self.client.post('/api/v1/conversations', json={}).json()
        def submit(fn, *args, **kwargs):
            queued.append((fn, args, kwargs))
        def failed_run(*args, **kwargs):
            TeamStore(kwargs['run_dir']).checkpoint('BLOCKED', error='test failure')
        with patch.object(self.app.state.executor, 'submit', side_effect=submit), \
                patch('dataflow_agents.web.Orchestrator') as orchestrator:
            orchestrator.return_value.run.side_effect = failed_run
            response = self.client.post(
                f"/api/v1/conversations/{conversation['conversation_id']}/messages",
                json={'content': '清洗 raw_content 的空格', 'input_rows': [{'raw_content': 'test'}]})
            self.assertEqual(response.status_code, 200, response.text)
            run_id = response.json()['run_id']
            work, args, kwargs = queued.pop(0)
            work(*args, **kwargs)
        self.assertEqual(len(queued), 1, queued)
        return run_id, queued[0]

    def test_queued_diagnosis_after_delete_cannot_resurrect_run(self):
        run_id, (analyse, args, kwargs) = self.queued_diagnosis()
        response = self.client.delete(f'/api/v1/runs/{run_id}')
        self.assertEqual(response.status_code, 200, response.text)
        with patch('dataflow_agents.web.build_backend') as backend:
            analyse(*args, **kwargs)
            backend.assert_not_called()
        self.assertFalse((self.root / run_id).exists())
        self.assertEqual(self.client.get('/api/v1/runs').status_code, 200)

    def test_running_diagnosis_blocks_delete_until_all_writes_finish(self):
        run_id, (analyse, args, kwargs) = self.queued_diagnosis()
        started, finish = threading.Event(), threading.Event()
        def ask(*args, **kwargs):
            started.set()
            if not finish.wait(5):
                raise AssertionError('Test did not release diagnostic worker')
            raise RuntimeError('Simulate unavailable analysis service')
        with patch('dataflow_agents.web.build_backend') as backend, ThreadPoolExecutor(1) as executor:
            backend.return_value.ask.side_effect = ask
            future = executor.submit(analyse, *args, **kwargs)
            try:
                self.assertTrue(started.wait(5))
                response = self.client.delete(f'/api/v1/runs/{run_id}')
                self.assertEqual(response.status_code, 409, response.text)
                self.assertTrue((self.root / run_id).exists())
            finally:
                finish.set()
            future.result(timeout=5)
        response = self.client.delete(f'/api/v1/runs/{run_id}')
        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse((self.root / run_id).exists())
