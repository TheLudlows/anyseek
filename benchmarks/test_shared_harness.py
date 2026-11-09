"""Regression checks for benchmark scheduling, evidence retention and retry safety."""
import json
import importlib.util
import threading
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from shared_record_http import request

ROOT = Path(__file__).resolve().parent


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.probe = self.root / 'probe'
        self.probe.write_text('''#!/usr/bin/env python3
import json,sys
from pathlib import Path
root=Path(sys.argv[1]);root.mkdir(exist_ok=True)
(root/'seed').write_text('fixed')
Path(sys.argv[4]).write_text(json.dumps({'rows':[{'result':42}]}))
''')
        self.probe.chmod(0o755)

    def run_probe(self, entry, output, after=None, *extra):
        return subprocess.run([sys.executable, str(ROOT / entry), '--before', str(self.probe),
            '--after', str(after or self.probe), '--output', str(output), '--workloads', '32:false',
            '--cpus', str(min(os.sched_getaffinity(0))), *extra], capture_output=True, text=True)

    def test_existing_defaults_and_resource_sidecars(self):
        for entry, rounds, resources in [('query-cost/compare.py', 1, False),
                                         ('query-alignment/allocation_compare.py', 3, True)]:
            with self.subTest(entry=entry):
                out = self.root / str(rounds)
                result = self.run_probe(entry, out)
                self.assertEqual(result.returncode, 0, result.stderr)
                rows = json.loads((out / 'results.json').read_text())['runs']
                self.assertEqual([r['version'] for r in rows], ['base', 'candidate', 'candidate', 'base'] * rounds)
                self.assertEqual(len(list(out.glob('result-*.resources'))), 4 * rounds if resources else 0)
                self.assertFalse(list(out.glob('run-*')))
                self.assertEqual(len({r['seed_sha256'] for r in rows}), 1)
                self.assertNotEqual(self.run_probe(entry, out).returncode, 0, 'must not overwrite evidence')

    def test_parity_failure_preserves_failed_database_and_prior_results(self):
        other = self.root / 'different'
        other.write_text(self.probe.read_text().replace('42', '43'))
        other.chmod(0o755)
        out = self.root / 'failure'
        result = self.run_probe('query-cost/compare.py', out, other)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('parity failure', result.stderr)
        self.assertTrue((out / 'run-32-False-1').is_dir())
        self.assertEqual(len(json.loads((out / 'results.json').read_text())['runs']), 1)

    def test_explicit_rounds_and_resources(self):
        out = self.root / 'override'
        result = self.run_probe('query-cost/compare.py', out, None, '--rounds', '2', '--resources')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(list(out.glob('result-*.resources'))), 8)


class RetryTests(unittest.TestCase):
    class Server:
        last_status = 200
        def __init__(self, responses):
            self.responses = iter(responses)
            self.bodies = []
        def call(self, method, path, body, allowed):
            self.bodies.append(body)
            response = next(self.responses)
            if isinstance(response, Exception):
                raise response
            return response, 1.0

    def test_busy_replays_same_ids_and_retains_attempts(self):
        body = {'records': [{'operation_id': {'epoch': 1, 'nonce': 'x'}}]}
        server = self.Server([{'code': 'busy'}, {'results': [{'code': 'committed'}]}])
        with patch('shared_record_http.time.sleep'):
            result, elapsed, attempts = request(server, 'POST', '/ingest', body)
        self.assertEqual(len(attempts), 2)
        self.assertTrue(all(sent is body for sent in server.bodies))
        self.assertEqual(result['results'][0]['code'], 'committed')

    def test_unknown_outcome_and_transport_error_never_retry(self):
        server = self.Server([{'results': [{'code': 'outcome_unknown'}, {'code': 'busy'}]}])
        _, _, attempts = request(server, 'POST', '/batch', {})
        self.assertEqual(len(attempts), 1)
        server = self.Server([ConnectionError('unknown transport outcome')])
        with self.assertRaises(ConnectionError):
            request(server, 'POST', '/batch', {})
        self.assertEqual(len(server.bodies), 1)

    def test_query_status_stays_thread_local(self):
        spec = importlib.util.spec_from_file_location('query_support', ROOT / 'query-alignment/support.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        server = module.Server.__new__(module.Server)
        server.local = threading.local()
        barrier = threading.Barrier(2)
        seen = []
        def worker(status):
            server.last_status = status
            barrier.wait()
            seen.append(server.last_status)
        threads = [threading.Thread(target=worker, args=(status,)) for status in (200, 429)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self.assertEqual(sorted(seen), [200, 429])
        self.assertIsNone(server.last_status)

    def test_changed_replay_receipt_fails(self):
        rows = [{'code': 'committed', 'operation_id': {'nonce': 'x'}, 'receipt': {'seq': 1}}, {'code': 'busy'}]
        changed = [dict(rows[0], receipt={'seq': 2})]
        server = self.Server([{'results': rows}, {'results': changed}])
        with patch('shared_record_http.time.sleep'), self.assertRaisesRegex(AssertionError, 'replayed receipt changed'):
            request(server, 'POST', '/batch', {})


if __name__ == '__main__':
    unittest.main()
