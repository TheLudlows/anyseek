#!/usr/bin/env python3
"""Exercise the Record client against a running server (default port 8080).

Set ANYSEEK_API_KEY to the startup master API key. --requests selects a JSON
file to save before sending, or to replay after a lost response. Each new file
creates a new collection. Full process/browser suite: record_service_smoke.py.
"""
import argparse
import json
import os
from pathlib import Path
import uuid

from anyseek_client import anyseekClient, check, operation_id


def exercise(base, key, request_file):
    client = anyseekClient(base)
    client.set_default_key(key)
    client.health()
    request_file = Path(request_file)
    if request_file.exists():
        saved = json.loads(request_file.read_text())
        check(saved['base'] == base, 'saved requests belong to another service URL')
    else:
        collection = 'c'+uuid.uuid4().hex
        documents = [
            {'title': 'first', 'body': 'the quick brown fox'},
            {'title': 'second', 'body': 'the lazy brown dog'},
            {'title': 'third', 'body': 'a quick fox jumps over the dog'},
        ]
        saved = {
            'base': base,
            'create': {'operation_id': operation_id(), 'collection_id': collection,
                       'config': {'index_name': 'write_query_smoke', 'schema': [
                           {'field': 'title', 'field_type': 'Text'},
                           {'field': 'body', 'field_type': 'Text'}]}},
            'writes': [{'operation_id': operation_id(), 'record_id': str(i),
                         'expected': 'Absent', 'document': doc}
                       for i, doc in enumerate(documents)],
        }
        with request_file.open('x') as output:
            json.dump(saved, output)
            output.flush()
            os.fsync(output.fileno())
    collection = saved['create']['collection_id']
    check(client.create_collection(saved['create'])['collection_id'] == collection, 'collection ID mismatch')
    receipt = client.write(collection, saved['writes'][0])
    check(client.write(collection, saved['writes'][0]) == receipt, 'retry changed receipt')
    result = client.batch(collection, {'writes': saved['writes'][1:]})
    check(not result['atomic'] and all(r['code'] == 'applied' for r in result['results']), f'batch failed: {result}')
    # No explicit checkpoint is needed for durability or visibility.
    result = client.query(collection, {'query': 'quick fox'})
    titles = sorted(hit['record']['document']['title'] for hit in result['hits'])
    check(result['count_total'] == 2 and titles == ['first', 'third'], f'query mismatch: {result}')
    seq = client.checkpoint(collection)['visible_seq']
    result = client.query(collection, {})
    check(seq == result['visible_seq'] == 3 and result['count_total'] == 3, f'checkpoint changed view: {result}')
    print(f'PASS Record client: stable IDs, retry, batch, immediate query, checkpoint ({collection})')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--requests', type=Path, required=True, help='save/replay original requests; no API key saved')
    args = parser.parse_args()
    key = os.environ.get('ANYSEEK_API_KEY')
    if not key:
        parser.error('set ANYSEEK_API_KEY to the startup master API key')
    exercise(f'http://127.0.0.1:{args.port}', key, args.requests)


if __name__ == '__main__':
    main()
