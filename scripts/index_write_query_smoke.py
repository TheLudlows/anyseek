#!/usr/bin/env python3
"""Create-index, write and query smoke test against a built seekstorm_server.

Requires only Python 3 and an already-built seekstorm_server binary (same
contract as scripts/http_smoke.py, no published client SDK). All writes go to a
fresh temporary index directory and the server is stopped even if an assertion
fails. Exercises the core create -> write -> commit -> query loop plus a
realtime query over an uncommitted write.

The REST surface is wrapped by SeekStormClient (scripts/seekstorm_client.py).
"""
import argparse
import base64
import hashlib
import os
import re
import secrets
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.error import URLError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from seekstorm_client import SeekStormClient, check


def exercise(binary, workdir):
    # Reserve an OS-selected high port until immediately before launching.
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    base = f'http://127.0.0.1:{port}'
    secret = secrets.token_hex(24)
    master = base64.b64encode(hashlib.sha256(secret.encode()).digest()).decode()
    client = SeekStormClient(base)

    log_path = workdir / 'server.log'
    with log_path.open('wb') as log:
        def launch():
            return subprocess.Popen(
                [str(binary), 'local_ip=127.0.0.1', f'local_port={port}',
                 f'index_path={workdir / "index"}', 'force_shard_number=1'],
                cwd=workdir, env={**os.environ, 'MASTER_KEY_SECRET': secret},
                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)

        def ready(process):
            deadline = time.monotonic() + 30
            while True:
                check(process.poll() is None, 'server exited before becoming ready')
                try:
                    client.health()
                    return
                except (URLError, TimeoutError, ConnectionError):
                    if time.monotonic() >= deadline:
                        raise RuntimeError('server did not become ready within 30 seconds')
                    time.sleep(0.1)

        process = launch()
        try:
            ready(process)
            print('PASS health')

            # First startup prints exactly one random initial API key.
            initial_keys = re.findall(r'^Initial API key: (.+)$',
                                      log_path.read_text(), re.MULTILINE)
            check(len(initial_keys) == 1, 'first startup did not generate exactly one key')
            initial_key = initial_keys[0]
            check(len(base64.b64decode(initial_key)) == 32,
                  'initial key is not a 256-bit credential')
            print('PASS first-run random API key')

            # The initial key is an ordinary scoped credential; only the
            # master key (derived from MASTER_KEY_SECRET) can mint new keys.
            api_key = client.create_api_key(master)
            check(len(base64.b64decode(api_key)) == 32,
                  'created API key is not a 256-bit credential')
            client.set_default_key(api_key)
            print('PASS API key creation')

            # Create an index with a stored, lexically indexed schema.
            schema = [
                {'field': 'title', 'field_type': 'Text', 'store': True, 'index_lexical': True},
                {'field': 'body', 'field_type': 'Text', 'store': True, 'index_lexical': True},
            ]
            index_id = client.create_index({
                'index_name': 'write_query_smoke',
                'schema': schema,
                'similarity': 'Bm25f',
                'tokenizer': 'UnicodeAlphanumeric',
                'stop_words': 'None',
                'frequent_words': 'English',
                'document_compression': 'Snappy',
                'inference': 'None',
                'clustering': 'None',
            })
            check(index_id == 0, f'first index ID mismatch: {index_id}')
            print(f'PASS index created (id={index_id})')

            # Write a single document, then a batch, and commit both.
            client.write_documents(index_id, {'title': 'first', 'body': 'the quick brown fox'})
            client.write_documents(index_id, [
                {'title': 'second', 'body': 'the lazy brown dog'},
                {'title': 'third', 'body': 'a quick fox jumps over the dog'},
            ])
            check(client.commit(index_id) == 3, 'commit must expose three documents')
            print('PASS document write and commit')

            # Query the committed index for a shared phrase.
            result = client.query(index_id, {
                'query': 'quick fox',
                'search_mode': 'Lexical',
                'result_type': 'TopkCount',
                'realtime': False,
            })
            check(result['count_total'] == 2, f'query count mismatch: {result}')
            titles = sorted(doc['title'] for doc in result['results'])
            check(titles == ['first', 'third'], f'query result mismatch: {result}')
            print('PASS lexical query over committed documents')

            # A realtime query sees a write that has not been committed yet.
            client.write_documents(index_id, {'title': 'fourth', 'body': 'uncommitted realtime record'})
            realtime = client.query(index_id, {
                'query': 'realtime',
                'search_mode': 'Lexical',
                'result_type': 'TopkCount',
                'realtime': True,
            })
            check(realtime['count_total'] == 1, f'realtime query mismatch: {realtime}')
            check(realtime['results'][0]['title'] == 'fourth',
                  f'realtime result mismatch: {realtime}')
            print('PASS realtime query over an uncommitted write')
        except BaseException:
            log.flush()
            print(log_path.read_text(errors='replace'))
            raise
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, required=True)
    args = parser.parse_args()
    binary = args.server.resolve()
    if not binary.is_file():
        parser.error(f'server binary not found: {binary}; run cargo build -p seekstorm_server')
    with tempfile.TemporaryDirectory(prefix='seekpower-write-query-') as directory:
        exercise(binary, Path(directory))
    print('PASS create/write/query smoke')


if __name__ == '__main__':
    main()
