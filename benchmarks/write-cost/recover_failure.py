#!/usr/bin/env python3
"""Recover the retained patch/durable_pending counterexample outside timing.

Requires a failed measure.py run with its disposable db still present. Copies
that database; never changes the original evidence or retries an unknown write
in the original process. This is not a general recovery/fault framework.
"""
import argparse
import ast
import gzip
import json
from pathlib import Path
import shutil
import traceback
import measure as m


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    args = parser.parse_args()
    if not __debug__:
        parser.error('recovery checks require assertions; do not use python -O')
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    summary = json.loads((args.source/'summary.json').read_text())
    # Initial driver retained the entire failed response in AssertionError repr.
    failure = summary.get('failure')
    response = failure['response'] if failure else ast.literal_eval(summary['error'][len('AssertionError('):-1])
    batch = summary['batch']
    epoch = response['results'][0]['operation_id']['epoch']
    kind, start = response['results'][0]['operation_id']['nonce'].rsplit('-', 1)
    assert kind == 'patch' and any(r['code'] == 'durable_pending' for r in response['results'])
    commands = m.commands(epoch, kind, int(start), batch)
    m.exclusive_process_check()
    manifest = m.files(args.source/'db')
    shutil.copytree(args.source/'db', args.out/'db')
    assert m.files(args.out/'db') == manifest
    result = {'source_run': summary['run'], 'database_manifest_sha256': m.digest(manifest),
              'original_response': response, 'status': 'running'}
    server = None
    try:
        server = m.Server(args.binary.resolve(), args.out/'db', args.out/'server.log', [0, 1])
        result['progress_before'] = server.call('GET', m.PATH+'/ingest')[0]
        statuses = [server.call('GET', m.PATH+f"/operation/{r['operation_id']['epoch']}/{r['operation_id']['nonce']}")[0] for r in commands]
        result['receipts_before_replay'] = statuses
        for old, status in zip(response['results'], statuses):
            if old.get('receipt'):
                assert old['receipt'] == status['receipt']
            if old['code'] == 'durable_pending':
                assert status['receipt'], status
        history = []
        with gzip.open(args.source/'requests.jsonl.gz', 'rt') as file:
            for line in file:
                row = json.loads(line)
                requests = m.commands(epoch, row['kind'], row['call']*batch, batch)
                assert m.digest(requests) == row['request_sha256']
                history.extend(zip(requests, row['receipts']))
        for offset in range(0, len(history), 64):
            part = history[offset:offset+64]
            assert m.write(server, 'batch', [r for r, _ in part])['receipts'] == [receipt for _, receipt in part]
        replay = m.write(server, 'batch', commands)
        for old, new in zip(statuses, replay['receipts']):
            if old['receipt']:
                assert old['receipt'] == new
        result['resolved_failed_batch'] = replay
        result['prior_receipts_replayed'] = len(history)
        inserted = {r['record_id'] for r, _ in history if 'document' in r}
        patched = {r['record_id'] for r, _ in history if 'vector_patch' in r} | {r['record_id'] for r in commands}
        found = server.query({'query': 'insertword', 'length': 1000, 'count': True})[0]
        assert {h['record']['record_id'] for h in found['hits']} == inserted
        found = server.query({'query_vector': m.V1, 'all': True, 'length': 1000,
                             'filter': [{'Int': {'field': 'group', 'filter': {'start': 1, 'end': 2}}}]})[0]
        assert {h['record']['record_id'] for h in found['hits']} == patched
        for identity in sorted(inserted):
            row = server.call('GET', m.PATH+'/doc/'+identity)[0]
            assert row['version'] == 1 and ('vector' in row['document']) == (identity in patched)
        result['query_validation'] = {'lexical_records': len(inserted), 'vector_records': len(patched),
                                      'record_checks': len(inserted)}
        result['progress_after'] = server.call('GET', m.PATH+'/ingest')[0]
        result['status'] = 'passed'
    except BaseException as error:
        result['error'] = repr(error)
        traceback.print_exc()
    finally:
        if server:
            server.close()
        m.dump(args.out/'recovery.json', result)
    if result['status'] != 'passed':
        raise SystemExit(1)
    shutil.rmtree(args.out/'db')
    print('verified recovery', result['query_validation'])


if __name__ == '__main__':
    main()
