#!/usr/bin/env python3
"""Create-index, write and query smoke test against a running seekstorm_server.

Requires only Python 3 and a seekstorm_server already listening on the local
default port (80). Start the server yourself first, e.g.:

    cargo run -p seekstorm_server

then run this script. All calls go through SeekStormClient
(scripts/seekstorm_client.py). The master key is derived from the
MASTER_KEY_SECRET environment variable (default "1234") exactly as the server
derives it, so this script authenticates against a server started without
explicitly setting that variable. Exercises the core create -> write -> commit
-> query loop plus a realtime query over an uncommitted write.

The REST surface is wrapped by SeekStormClient (scripts/seekstorm_client.py).
"""
import argparse
import sys
import time
from pathlib import Path
from urllib.error import URLError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from seekstorm_client import SeekStormClient, check

# Hardcoded master key = base64(sha256("1234")), matching a server started
# without setting MASTER_KEY_SECRET (whose default is "1234"). Authenticate
# against such a server without depending on the environment.
MASTER_KEY = 'A6xnQhbz4Vx2HuGl4lXwZ5U2I8iziLRFnhP5eNfIRvQ='


def exercise(base, master):
    client = SeekStormClient(base)

    # The server is started by the caller; wait for it to answer health checks.
    deadline = time.monotonic() + 30
    while True:
        try:
            client.health()
            break
        except (URLError, TimeoutError, ConnectionError):
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f'server not reachable at {base} within 30 seconds; '
                    'start it first with `cargo run -p seekstorm_server`')
            time.sleep(0.1)
    print('PASS health')

    # The master key is itself a full scoped key: create/write/query use it
    # directly, no separate API key needs to be minted first.
    client.set_default_key(master)

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
    check(isinstance(index_id, int),
          f'index ID is not an integer: {index_id!r}')
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8080,
                        help='local port the server is listening on (default: 80)')
    args = parser.parse_args()
    base = f'http://127.0.0.1:{args.port}'
    exercise(base, MASTER_KEY)
    print('PASS create/write/query smoke')


if __name__ == '__main__':
    main()
