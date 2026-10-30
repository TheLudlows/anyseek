#!/usr/bin/env python3
"""Run shared Record acceptance and S3 contracts against a fixture or test bucket.

Real mode accepts ONLY ANYSEEK_S3_TEST_* configuration. Use a dedicated disposable
bucket: end-to-end tests leave isolated test namespaces for provider-side disposal.
Credentials are inherited through the environment and never added to reports.
"""
import argparse
import hashlib
import http.server
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import tempfile
import time
from urllib.parse import urlsplit

from s3_storage_smoke import S3


def configuration(backend):
    if backend == 'fixture':
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), S3)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        values = {'ENDPOINT': f'http://127.0.0.1:{server.server_port}', 'BUCKET': 'disposable',
                  'REGION': 'us-east-1', 'ACCESS_KEY_ID': 'fixture',
                  'SECRET_ACCESS_KEY': 'fixture-secret', 'ALLOW_HTTP': '1'}
        return values, server
    required = ['ENDPOINT', 'BUCKET', 'REGION', 'ACCESS_KEY_ID', 'SECRET_ACCESS_KEY']
    missing = [f'ANYSEEK_S3_TEST_{key}' for key in required if not os.environ.get(f'ANYSEEK_S3_TEST_{key}')]
    if missing:
        raise ValueError('Missing test configuration: ' + ', '.join(missing))
    values = {key: os.environ[f'ANYSEEK_S3_TEST_{key}'] for key in required}
    values['ALLOW_HTTP'] = os.environ.get('ANYSEEK_S3_TEST_ALLOW_HTTP', '0')
    endpoint = urlsplit(values['ENDPOINT'])
    if endpoint.scheme not in ('https', 'http') or not endpoint.hostname or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
        raise ValueError('Test endpoint must be an HTTP(S) URL without credentials, query or fragment')
    if endpoint.scheme == 'http' and values['ALLOW_HTTP'] != '1':
        raise ValueError('An HTTP test endpoint requires ANYSEEK_S3_TEST_ALLOW_HTTP=1')
    return values, None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backend', choices=['fixture', 'real'], default='fixture')
    parser.add_argument('--suite', choices=['semantic', 'storage', 'contract', 'all'], default='all')
    parser.add_argument('--server', help='Fixed service binary; build fault-injection for --faults')
    parser.add_argument('--faults', action='store_true')
    parser.add_argument('--report', type=Path, help='Optional JSON report, excluding credentials')
    args = parser.parse_args()
    if args.suite != 'contract' and (not args.server or not Path(args.server).is_file()):
        parser.error('--server must identify an existing binary')
    try:
        values, fixture = configuration(args.backend)
    except ValueError as error:
        parser.error(str(error))
    root = Path(__file__).resolve().parents[1]
    env = {**os.environ, 'ANYSEEK_STORAGE_BACKEND': 's3', 'PYTHONUNBUFFERED': '1'}
    for key, value in values.items():
        env[f'ANYSEEK_S3_{key}'] = value
        env[f'ANYSEEK_S3_TEST_{key}'] = value
    if fixture:
        env['ANYSEEK_S3_TEST_RESTORE_BUCKET'] = 'disposable-restored'
    report = {'backend': args.backend, 'real_endpoint_exercised': args.backend == 'real',
              'fault_injection': args.faults, 'suite': args.suite, 'checks': []}
    fixed_binary = tempfile.TemporaryDirectory(prefix='anyseek-acceptance-binary-')
    if args.server:
        binary = Path(fixed_binary.name)/Path(args.server).name
        shutil.copy2(Path(args.server).resolve(), binary)
        with binary.open('rb') as source:
            digest = hashlib.sha256()
            for block in iter(lambda: source.read(1024 * 1024), b''):
                digest.update(block)
            report['server_sha256'] = digest.hexdigest()
    else:
        binary = None
    commands = []
    if args.suite in ['contract', 'all']:
        cargo = shutil.which('cargo')
        if not cargo:
            parser.error('cargo is required for the contract suite')
        commands.append(('object_contract', [cargo, 'test', '-p', 'anyseek', '--lib', '--features', 'server,fault-injection',
                         'real_s3_compatible_contract', '--', '--ignored']))
    if args.suite in ['semantic', 'all']:
        for section in ['base', 'locator', 'graph', 'temporal', 'upload', 'lease', 'history']:
            commands.append(('record_'+section+'_cold_cache', [sys.executable, '-u', str(root/'scripts/record_service_smoke.py'),
                             '--server', str(binary), '--cold-restarts', '--section', section] + (['--faults'] if args.faults else [])))
    if args.suite in ['storage', 'all']:
        commands.append(('checkpoint_backup_restore', [sys.executable, '-u', str(root/'scripts/s3_storage_smoke.py'),
                         '--server', str(binary), '--backend', args.backend] + (['--faults'] if args.faults else [])))
    try:
        for name, command in commands:
            if fixture:
                with S3.lock:S3.objects.clear()
            print(f'RUN {name} backend={args.backend}', flush=True)
            started = time.monotonic()
            result = subprocess.run(command, cwd=root, env=env)
            report['checks'].append({'name': name, 'exit_code': result.returncode,
                                     'seconds': round(time.monotonic()-started, 3)})
            if result.returncode:
                break
        report['all_requested_checks_passed'] = len(report['checks']) == len(commands) and all(check['exit_code'] == 0 for check in report['checks'])
    finally:
        fixed_binary.cleanup()
        if fixture:
            fixture.shutdown()
            fixture.server_close()
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(report, ensure_ascii=False), flush=True)
    return 0 if report['all_requested_checks_passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
