#!/usr/bin/env python3
"""POSIX SIGTERM, disconnected accepted writes, native drain and immediate reopen.

Requires a fault-injection server; all data and pause markers are disposable.
"""
import argparse
import base64
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time

from anyseek_client import anyseekClient, operation_id


def until(predicate, message, seconds=15):
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError(message)
        time.sleep(.02)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', required=True)
    args = parser.parse_args()
    assert os.name == 'posix', 'SIGTERM smoke requires POSIX'
    binary = str(Path(args.server).resolve())
    with tempfile.TemporaryDirectory(prefix='anyseek-shutdown-', dir=os.environ.get('ANYSEEK_TEST_ROOT')) as tmp:
        root = Path(tmp) / 'data'
        pause = Path(tmp) / 'authorized-pause'
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
        secret = 'issue167-disposable-shutdown'
        master = base64.b64encode(hashlib.sha256(secret.encode()).digest()).decode()
        key = base64.b64encode(hashlib.sha256(b'issue167-tenant').digest()).decode()
        env = {**os.environ, 'MASTER_KEY_SECRET': secret, 'ANYSEEK_STORAGE_BACKEND': 'local'}
        client = anyseekClient(f'http://127.0.0.1:{port}')
        client.set_default_key(key)
        process = None
        keepalive = []
        with (Path(tmp) / 'server.log').open('w+') as log:
            def start(faults=False):
                nonlocal process
                additions = {'ANYSEEK_AUTHORIZED_PAUSE_COLLECTION': 'shutdown',
                             'ANYSEEK_AUTHORIZED_PAUSE_FILE': str(pause)} if faults else {}
                process = subprocess.Popen([binary, f'index_path={root}', f'local_port={port}',
                                            'local_ip=127.0.0.1'], env={**env, **additions}, stdout=log, stderr=log)

                def ready():
                    assert process.poll() is None, 'server exited during startup'
                    try:
                        client.health()
                        return True
                    except OSError:
                        return False
                until(ready, 'server did not become ready')

            def hold_keepalive():
                for _ in range(4):
                    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
                    connection.request('GET', '/api/v1/live')
                    response = connection.getresponse()
                    assert response.status == 200
                    response.read()
                    keepalive.append(connection)

            def stop():
                process.send_signal(signal.SIGTERM)
                assert process.wait(timeout=15) == 0, 'graceful shutdown failed'
                for connection in keepalive:
                    connection.close()
                keepalive.clear()

            try:
                pause.write_text('pause')
                start(faults=True)
                client.create_tenant({'operation_id': operation_id(), 'tenant_id': 'test'}, key=master)
                client.register_key({'operation_id': operation_id(), 'tenant_id': 'test',
                                     'key_id': 'tenant', 'apikey_base64': key}, key=master)
                client.create_collection({'operation_id': operation_id(), 'collection_id': 'shutdown',
                                          'config': {'index_name': 'Shutdown', 'schema': [
                                              {'field': 'body', 'field_type': 'Text', 'store': True}]}})
                partitions = list(root.glob('collections/*/*/*'))
                assert len(partitions) == 1, partitions
                partition = partitions[0]
                for lane in ('lexical', 'vector'):
                    (partition / f'.test-{lane}-consumer-pause').write_text('pause')
                command = {'operation_id': operation_id(), 'record_id': 'accepted', 'expected': 'Absent',
                           'document': {'body': 'alpha'}}
                payload = json.dumps({'records': [command]}).encode()
                hold_keepalive()
                with socket.create_connection(('127.0.0.1', port), timeout=5) as connection:
                    connection.sendall((f'POST /api/v1/index/shutdown/ingest HTTP/1.1\r\nHost: localhost\r\n'
                                        f'apikey: {key}\r\nContent-Type: application/json\r\n'
                                        f'Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n').encode() + payload)
                    until(lambda: pause.with_suffix('.started').exists(), 'write was not admitted')
                # The HTTP caller is gone, but its accepted owned write must finish.
                process.send_signal(signal.SIGTERM)
                time.sleep(.1)
                assert process.poll() is None, 'shutdown discarded an accepted write'
                contender = subprocess.run([binary, f'index_path={root}', 'local_port=0', 'local_ip=127.0.0.1'],
                                           env=env, stdout=log, stderr=log, timeout=10)
                assert contender.returncode != 0, 'root unlocked before drain'
                pause.unlink()
                until(lambda: all((partition / f'.test-{lane}-consumer-started').exists()
                                  for lane in ('lexical', 'vector')), 'index consumers did not start')
                assert process.poll() is None, 'shutdown discarded active consumers'
                for lane in ('lexical', 'vector'):
                    (partition / f'.test-{lane}-consumer-pause').unlink()
                assert process.wait(timeout=15) == 0, 'drain did not finish'
                for connection in keepalive:
                    connection.close()
                keepalive.clear()

                # Reopen immediately, retain the operation identity, and verify recovery.
                start()
                receipt = client.operation_status('shutdown', command['operation_id'])['receipt']
                assert receipt['seq'] == 1, receipt
                assert client.ingest('shutdown', [command])['results'][0]['receipt'] == receipt
                assert client.query('shutdown', {'query': 'alpha'})['count_total'] == 1
                hold_keepalive()
                stop()
                log.seek(0)
                assert log.read().count('Shutdown complete') == 2
                print('PASS SIGTERM: cancelled accepted write, paused consumers, keepalive, root ownership and same-ID reopen')
            except BaseException:
                log.seek(0)
                # The test secret is disposable; omit the printed API key anyway.
                print('\n'.join(line for line in log.read().splitlines() if 'Master API key:' not in line))
                raise
            finally:
                for connection in keepalive:
                    connection.close()
                if process is not None and process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)


if __name__ == '__main__':
    main()
