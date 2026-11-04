#!/usr/bin/env python3
"""Fixed-work HTTP concurrency probe; retain every response, including failures."""
import argparse
import concurrent.futures
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'write-cost'))
import measure as base


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--seed', type=Path, help='Reuse an identical baseline checkpoint')
    parser.add_argument('--endpoint', choices=['batch', 'ingest'], default='batch')
    parser.add_argument('--mode', choices=['lexical', 'hybrid'], default='lexical')
    parser.add_argument('--phased', action='store_true', help='Synchronize every batch with eight queries')
    args = parser.parse_args()
    assert __debug__, 'assertions required'
    os.sched_setaffinity(0, {2, 3})
    lock = open('/tmp/anyseek-performance.lock', 'w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    base.exclusive_process_check()
    args.out.mkdir(parents=True, exist_ok=False)
    epoch = int(time.time() // 86400)
    binary = args.binary.resolve()
    seed = args.seed or base.prepare(binary, args.out, 1000, [0, 1], epoch)
    seed_info = json.loads((seed.parent / 'seed-1000.json').read_text())
    assert seed_info['epoch'] == epoch and seed_info['files'] == base.files(seed)
    db = args.out / 'db'
    shutil.copytree(seed, db)
    server = base.Server(binary, db, args.out / 'server.log', [0, 1])
    records = []
    start = time.perf_counter()
    barrier = threading.Barrier(5)
    query = {'query': 'seedword', 'length': 10}
    if args.mode == 'hybrid':
        query['query_vector'] = base.V1

    def worker(kind, worker_id, count):
        conn = http.client.HTTPConnection('127.0.0.1', server.port, timeout=120)
        barrier.wait()
        for i in range(count):
            if args.phased and (kind == 'write' or i % 2 == 0):
                barrier.wait()
            body = query if kind == 'query' else {
                'writes' if args.endpoint == 'batch' else 'records': [
                    {'operation_id': base.oid(epoch, f'mixed-{i}-{j}'),
                     'record_id': f'mixed-{i}-{j}', 'expected': 'Absent',
                     'document': {'body': 'mixedword ' + 'padding ' * 30,
                                  'group': 1, 'vector': base.V1}}
                    for j in range(16)]}
            path = base.PATH + '/' + ('query' if kind == 'query' else args.endpoint)
            began = time.perf_counter()
            row = {'kind': kind, 'worker': worker_id, 'i': i, 'start_s': began - start}
            try:
                conn.request('POST', path, json.dumps(body), {'apikey': base.KEY, 'Content-Type': 'application/json'})
                response = conn.getresponse()
                row['status'] = response.status
                row['body'] = response.read().decode()
            except (OSError, http.client.HTTPException) as error:
                row['transport_error'] = repr(error)
                conn.close()
                conn = http.client.HTTPConnection('127.0.0.1', server.port, timeout=120)
            row['ms'] = (time.perf_counter() - began) * 1000
            records.append(row)
            if args.phased and (kind == 'write' or i % 2 == 1):
                barrier.wait()
        conn.close()

    try:
        before = base.resource(server)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            jobs = [pool.submit(worker, 'write', 0, 40)]
            jobs += [pool.submit(worker, 'query', i, 80) for i in range(4)]
            for job in jobs:
                job.result()
        after = base.resource(server)
        base.exclusive_process_check(server.proc_pid)
        immediate_progress, _ = server.call('GET', base.PATH + '/ingest')
        catchup_ms, polls, progress = base.wait_cut(server, 1640)
        final, _ = server.query({'query': 'mixedword', 'length': 0})
        summary = {'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                   'seed_sha256': base.digest(seed_info['files']),
                   'harness_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   'endpoint': args.endpoint, 'mode': args.mode, 'progress': progress,
                   'phased': args.phased,
                   'immediate_progress': immediate_progress, 'catchup_ms': catchup_ms, 'catchup_polls': polls,
                   'final_count': final['count_total'], 'before': before, 'after': after}
        for kind in ['query', 'write']:
            rows = [r for r in records if r['kind'] == kind]
            success = [r for r in rows if r.get('status') == 200 and 'transport_error' not in r]
            errors = {}
            for r in rows:
                if r not in success:
                    key = r.get('transport_error', f"{r.get('status')} {r.get('body')}")
                    errors[key] = errors.get(key, 0) + 1
            summary[kind] = {'count': len(rows), 'success': len(success), 'errors': errors,
                             'all': base.quantiles([r['ms'] for r in rows]),
                             'successful': base.quantiles([r['ms'] for r in success]) if success else None}
        writes = [r for r in records if r['kind'] == 'write']
        writer_end = max(r['start_s'] + r['ms'] / 1000 for r in writes)
        summary['queries_started_before_writer_end'] = sum(r['kind'] == 'query' and r['start_s'] < writer_end for r in records)
        base.dump(args.out / 'summary.json', summary)
        print(json.dumps(summary, indent=2))
        assert summary['write']['success'] == 40, 'write HTTP failure; inspect raw evidence'
        for row in records:
            if row['kind'] == 'write':
                results = json.loads(row['body'])['results']
                assert len(results) == 16 and all(r['code'] == ('applied' if args.endpoint == 'batch' else 'committed') for r in results), results
        assert final['count_total'] == 640 and not progress['index_error'] and not progress['recovery_required'], summary
    finally:
        with (args.out / 'requests.jsonl').open('w') as raw:
            for row in sorted(records, key=lambda r: r['start_s']):
                raw.write(json.dumps(row) + '\n')
        server.close()


if __name__ == '__main__':
    main()
