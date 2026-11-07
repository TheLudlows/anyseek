#!/usr/bin/env python3
"""Small cross-platform local write probe; raw HTTP timings, not an SLA test."""
import argparse
import base64
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import uuid

if not __debug__:
    raise SystemExit('publication_probe requires assertions; do not use python -O')


def operation():
    return {'epoch': int(time.time() // 86400), 'nonce': uuid.uuid4().hex}


def run(binary, batch, calls, repetition):
    with tempfile.TemporaryDirectory(prefix='publication-probe-') as directory:
        root = Path(directory)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        env = {k: v for k, v in os.environ.items() if not k.startswith('ANYSEEK_')}
        env.update(MASTER_KEY_SECRET='local-publication-probe', ANYSEEK_STORAGE_BACKEND='local')
        master = base64.b64encode(hashlib.sha256(b'local-publication-probe').digest()).decode()
        tenant = base64.b64encode(hashlib.sha256(b'publication-probe-tenant').digest()).decode()
        with (root / 'server.log').open('w+') as log:
            process = subprocess.Popen([str(binary), f'index_path={root / "data"}',
                                        f'local_port={port}', 'local_ip=127.0.0.1'],
                                       env=env, stdout=log, stderr=log)
            connection = http.client.HTTPConnection('127.0.0.1', port, timeout=40)
            try:
                deadline = time.monotonic() + 30
                while True:
                    try:
                        connection.request('GET', '/api/v1/live')
                        response = connection.getresponse()
                        response.read()
                        assert response.status == 200
                        break
                    except OSError:
                        connection.close()
                        if process.poll() is not None or time.monotonic() >= deadline:
                            log.seek(0)
                            raise RuntimeError('service startup failed: ' + log.read()[-8192:])
                        time.sleep(.05)

                def call(method, path, body, key=tenant):
                    encoded = json.dumps(body).encode()
                    started = time.perf_counter()
                    connection.request(method, path, encoded,
                                       {'apikey': key, 'Content-Type': 'application/json'})
                    response = connection.getresponse()
                    raw = response.read()
                    elapsed = (time.perf_counter() - started) * 1000
                    assert response.status == 200, (path, response.status, raw)
                    return json.loads(raw), elapsed

                call('POST', '/api/v1/tenants',
                     {'operation_id': operation(), 'tenant_id': 'probe'}, master)
                call('POST', '/api/v1/apikey',
                     {'operation_id': operation(), 'tenant_id': 'probe', 'key_id': 'probe',
                      'apikey_base64': tenant}, master)
                call('POST', '/api/v1/index',
                     {'operation_id': operation(), 'collection_id': 'probe',
                      'config': {'index_name': 'probe', 'schema': [
                          {'field': 'body', 'field_type': 'Text', 'store': True}]}})
                timings = []
                attempts = []
                last = None
                for n in range(calls + 3):
                    writes = [{'operation_id': operation(), 'record_id': f'r-{n}-{i}',
                               'expected': 'Absent', 'document': {'body': 'alpha beta ' * 40}}
                              for i in range(batch)]
                    started = time.perf_counter()
                    retry_deadline = time.monotonic() + 30
                    statuses = []
                    while True:
                        result, _ = call('POST', '/api/v1/index/probe/batch', {'writes': writes})
                        assert len(result['results']) == batch, result
                        codes = [row['code'] for row in result['results']]
                        statuses.append(codes)
                        if all(code == 'applied' for code in codes):
                            break
                        # Replay the identical request, including already applied
                        # rows. Never retry an unknown outcome or business error.
                        assert all(code in ('applied', 'busy') for code in codes), result
                        assert time.monotonic() < retry_deadline, ('Busy retry deadline', codes)
                        time.sleep(.01)
                    elapsed = (time.perf_counter() - started) * 1000
                    if n >= 3:
                        timings.append(elapsed)
                        attempts.append(statuses)
                    last = (writes, result)
                result, _ = call('POST', '/api/v1/index/probe/query', {'query': 'alpha', 'length': 0})
                assert result['count_total'] == (calls + 3) * batch, result
                replay, _ = call('POST', '/api/v1/index/probe/batch', {'writes': last[0]})
                assert replay['results'] == last[1]['results'], (last[1], replay)
                ordered = sorted(timings)
                return {'batch': batch, 'repetition': repetition, 'calls': calls,
                        'latency_ms': timings, 'median_ms': ordered[len(ordered) // 2],
                        'attempt_codes': attempts,
                        'p95_ms': ordered[min(len(ordered) - 1, int(len(ordered) * .95))],
                        'items_per_second': calls * batch / (sum(timings) / 1000),
                        'query_count_and_replay_verified': True}
            finally:
                connection.close()
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=20)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--batches', default='1,16,64')
    parser.add_argument('--calls', type=int, default=30)
    parser.add_argument('--repetitions', type=int, default=3)
    args = parser.parse_args()
    batches = [int(value) for value in args.batches.split(',')]
    if not batches or any(not 1 <= value <= 64 for value in batches) or args.calls < 1 or args.repetitions < 1:
        parser.error('require batches within 1..64 and positive calls/repetitions')
    binary = args.binary.resolve(strict=True)
    if args.out.exists():
        parser.error('output already exists; use a new report path')
    report = {'binary': str(binary), 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'scope': 'local fresh roots, sequential HTTP, debug/release determined by binary; no saturation/RSS/SLA claim',
              'all_requested_runs_completed': False, 'runs': []}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    for repetition in range(args.repetitions):
        for batch in batches:
            result = run(binary, batch, args.calls, repetition)
            report['runs'].append(result)
            args.out.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
            print(json.dumps({k: v for k, v in result.items() if k not in ('latency_ms', 'attempt_codes')}), flush=True)
    report['all_requested_runs_completed'] = True
    args.out.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
