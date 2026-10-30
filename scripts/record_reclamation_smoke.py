#!/usr/bin/env python3
"""Deleted incarnation reclamation over real HTTP, local or in-memory S3 fixture."""
import argparse
import base64
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', required=True)
    parser.add_argument('--backend', choices=['local', 'fixture'], default='local')
    args = parser.parse_args()
    binary = str(Path(args.server).resolve())
    fixture = None
    env = {**os.environ, 'MASTER_KEY_SECRET': 'reclamation-smoke', 'ANYSEEK_STORAGE_BACKEND': 'local'}
    if args.backend == 'fixture':
        from s3_storage_smoke import S3
        import http.server
        fixture = http.server.ThreadingHTTPServer(('127.0.0.1', 0), S3)
        threading.Thread(target=fixture.serve_forever, daemon=True).start()
        env.update(ANYSEEK_STORAGE_BACKEND='s3', ANYSEEK_S3_ENDPOINT=f'http://127.0.0.1:{fixture.server_port}',
                   ANYSEEK_S3_BUCKET='reclamation', ANYSEEK_S3_REGION='us-east-1',
                   ANYSEEK_S3_ACCESS_KEY_ID='fixture', ANYSEEK_S3_SECRET_ACCESS_KEY='fixture', ANYSEEK_S3_ALLOW_HTTP='1')
    master = base64.b64encode(hashlib.sha256(b'reclamation-smoke').digest()).decode()
    tenant = base64.b64encode(hashlib.sha256(b'reclamation-tenant').digest()).decode()
    operation = lambda: {'epoch': int(time.time() // 86400), 'nonce': uuid.uuid4().hex}
    with tempfile.TemporaryDirectory(prefix='anyseek-reclamation-') as temporary:
        root = Path(temporary) / 'data'
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        base = f'http://127.0.0.1:{port}'
        log = open(Path(temporary) / 'server.log', 'w+')
        process = None

        def call(method, path, body=None, key=tenant, expected=(200,)):
            request = urllib.request.Request(base + path, method=method,
                data=None if body is None else json.dumps(body).encode(),
                headers={'apikey': key, 'Content-Type': 'application/json'})
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    status, raw = response.status, response.read()
            except urllib.error.HTTPError as error:
                status, raw = error.code, error.read()
            assert status in expected, (method, path, status, raw)
            return json.loads(raw)

        def wait(predicate):
            deadline = time.monotonic() + 45
            while not predicate():
                assert time.monotonic() < deadline, 'reclamation progress deadline'
                assert process.poll() is None, 'service exited'
                time.sleep(.05)

        def start():
            nonlocal process
            process = subprocess.Popen([binary, f'index_path={root}', f'local_port={port}', 'local_ip=127.0.0.1'],
                                       env=env, stdout=log, stderr=log)
            def ready():
                try:
                    with urllib.request.urlopen(base + '/api/v1/live', timeout=1):
                        return True
                except (OSError, urllib.error.URLError):
                    return False
            wait(ready)

        def metadata():
            with closing(sqlite3.connect(root / 'metadata.sqlite')) as db:
                return [json.loads(row[0]) for row in db.execute('SELECT metadata FROM catalog_collections')]

        try:
            start()
            call('POST', '/api/v1/tenants', {'operation_id': operation(), 'tenant_id': 'test'}, master)
            call('POST', '/api/v1/apikey', {'operation_id': operation(), 'key_id': 'tenant', 'tenant_id': 'test',
                                          'apikey_base64': tenant}, master)
            create = {'operation_id': operation(), 'collection_id': 'c',
                      'config': {'index_name': 'reclamation', 'schema': [{'field': 'body', 'field_type': 'Text'}]}}
            call('POST', '/api/v1/index', create)
            call('POST', '/api/v1/index/c/doc', {'operation_id': operation(), 'record_id': 'r', 'routing_key': 'r',
                                               'expected': 'Absent', 'document': {'body': 'old'}})
            old = metadata()[0]
            generation = f"{old['generation']:016x}"
            old_path = root / 'collections' / 'test' / 'c' / generation
            backup = call('POST', '/api/v1/index/c/backup', {'operation_id': operation()})['backup_id']
            delete = {'operation_id': operation()}
            call('DELETE', '/api/v1/index/c', delete)
            create['operation_id'] = operation()
            call('POST', '/api/v1/index', create)
            call('POST', '/api/v1/index/c/doc', {'operation_id': operation(), 'record_id': 'r', 'routing_key': 'r',
                                               'expected': 'Absent', 'document': {'body': 'new'}})
            time.sleep(1.2)
            assert old_path.exists(), 'live backup must protect the deleted incarnation'
            assert not next(row for row in metadata() if row['generation'] == old['generation'])['reclaiming']
            if fixture:
                # Fault injection: an accepted PUT whose completion is unknown.
                late_key = f'anyseek/test/c/records/{generation}-late-put'
                late_body = b'late remote arrival'
                with closing(sqlite3.connect(root / 'metadata.sqlite')) as db, db:
                    db.execute('INSERT INTO object_uploads VALUES(?,?,?,?,?)',
                               (late_key, hashlib.sha256(late_body).digest(), len(late_body), 0, 0))
            call('DELETE', '/api/v1/backups/' + backup, {'operation_id': operation()}, master)
            wait(lambda: all(row['generation'] != old['generation'] for row in metadata()))
            assert not old_path.exists()
            assert not list(old_path.parent.glob(f'.retired*{generation}'))
            if fixture:
                with S3.lock:
                    assert not any('/' + generation + '-' in key for key in S3.objects), 'old objects remain'
                    S3.objects['/reclamation/' + late_key] = late_body
                def late_deleted():
                    with S3.lock:
                        return '/reclamation/' + late_key not in S3.objects
                wait(late_deleted)
            call('DELETE', '/api/v1/index/c', delete)  # exact old receipt cannot delete the replacement
            assert call('GET', '/api/v1/index/c/doc/r')['document']['body'] == 'new'
            process.kill(); process.wait(timeout=20)
            start()
            assert len(metadata()) == 1
            assert call('GET', '/api/v1/index/c/doc/r')['document']['body'] == 'new'
            call('DELETE', '/api/v1/index/c', delete)
            assert call('GET', '/api/v1/index/c/doc/r')['document']['body'] == 'new'
            print(f'PASS {args.backend} reclamation: backup protection, same-name isolation, bounded eventual deletion, restart and old receipt fence')
        except Exception:
            log.flush(); log.seek(0)
            print(log.read()[-12000:])
            raise
        finally:
            if process and process.poll() is None:
                process.kill(); process.wait(timeout=20)
            log.close()
            if fixture:
                fixture.shutdown()
                fixture.server_close()


if __name__ == '__main__':
    main()
