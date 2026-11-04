#!/usr/bin/env python3
"""Issue 153: paired, warmed HTTP measurements on immutable binaries and snapshots.

Only disposable local servers are supported. No running user server is touched.
Latency includes HTTP I/O, excludes response comparison; CPU is server process
user+system time from /proc, divided by successful requests. Closed-loop load.
"""
import argparse
import base64
import concurrent.futures
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import shutil
import socket
import struct
import subprocess
import threading
import time
import uuid

SECRET = 'disposable-issue153-benchmark'
MASTER = base64.b64encode(hashlib.sha256(SECRET.encode()).digest()).decode()
KEY = base64.b64encode(hashlib.sha256(b'issue153-tenant').digest()).decode()
HERE = Path(__file__).resolve().parent


def dump(path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + '\n')


def operation():
    return {'epoch': int(time.time() // 86400), 'nonce': uuid.uuid4().hex}


def cpu(pid):
    values = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return (int(values[11]) + int(values[12])) / os.sysconf('SC_CLK_TCK')


def proc_pid(binary, data):
    # Some managed executors expose host /proc while subprocess uses namespace PIDs.
    # Resolve the exact disposable server by two unique command-line arguments.
    wanted = {str(binary).encode(), f'index_path={data}'.encode()}
    matches = []
    for path in Path('/proc').glob('[0-9]*'):
        try:
            if wanted.issubset(set((path/'cmdline').read_bytes().split(b'\0'))):
                matches.append(int(path.name))
        except (OSError, PermissionError):
            pass
    if len(matches) != 1:
        raise RuntimeError(('ambiguous server /proc identity', matches))
    return matches[0]


def allocations(path):
    return struct.unpack('QQ', path.read_bytes()[:16])


def quantiles(values):
    values = sorted(values)
    return {f'p{p}_ms': values[math.ceil(len(values)*p/100)-1] for p in (50, 95, 99)}


def signature(response):
    # Exclude time/clock and physical diagnostics, preserve complete business hits,
    # score values, channel explanations, count, watermark and query terms.
    return {k: response[k] for k in ('hits', 'count', 'count_total', 'visible_seq', 'query_terms', 'fusion')}


class Server:
    def __init__(self, binary, data, log, cpus, allocation_file=None):
        self.local = threading.local()
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]
        env = {**os.environ, 'MASTER_KEY_SECRET': SECRET,
               'TOKIO_WORKER_THREADS': '2', 'RAYON_NUM_THREADS': '2'}
        if allocation_file:
            env.update(LD_PRELOAD=str(HERE/'alloc.so'), ISSUE153_ALLOC_FILE=str(allocation_file))
        self.log = log.open('w')
        self.p = subprocess.Popen(['taskset', '-c', ','.join(map(str, cpus)), str(binary),
                                   f'index_path={data}', f'local_port={self.port}',
                                   'local_ip=127.0.0.1'], env=env, stdout=self.log, stderr=self.log)
        try:
            deadline = time.monotonic()+90
            while time.monotonic() < deadline:
                if self.p.poll() is not None:
                    raise RuntimeError(f'server exited: {log.read_text()[-4000:]}')
                try:
                    self.call('GET', '/api/v1/live')
                    self.proc_pid = proc_pid(binary, data)
                    return
                except (OSError, http.client.HTTPException):
                    time.sleep(.05)
            raise TimeoutError('startup')
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait()
        self.log.close()

    def call(self, method, path, body=None, key=KEY, allowed=(200,)):
        conn = getattr(self.local, 'connection', None)
        if conn is None:
            conn = self.local.connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=120)
        payload = None if body is None else json.dumps(body, separators=(',', ':')).encode()
        started = time.perf_counter_ns()
        try:
            conn.request(method, path, payload, {'Content-Type': 'application/json', 'apikey': key})
            response = conn.getresponse()
            raw = response.read()
            elapsed = (time.perf_counter_ns()-started)/1e6
        except BaseException:
            conn.close()
            self.local.connection = None
            raise
        if response.status not in allowed:
            raise RuntimeError((method, path, response.status, raw[:2000]))
        return json.loads(raw), elapsed

    def query(self, query):
        return self.call('POST', '/api/v1/index/bench/query', query)


def corpus_row(i, n):
    # Reverse insertion of business IDs, varied BM25 scores and an all-tied term.
    repeats = i % 8 + 1
    return {'record_id': f'r{n-i:08d}', 'expected': 'Absent',
            'document': {'body': ' '.join(['common']*repeats + ['padding']*(8-repeats) + ['tied']),
                         'priority': i, 'vector': [1.0, 0.0]}}


def prepare(binary, out, n, cpus):
    seed = out/f'seed-{n}'
    if (out/f'seed-{n}.json').exists():
        return seed
    if seed.exists():
        shutil.rmtree(seed)
    server = Server(binary, seed, out/f'prepare-{n}.log', cpus)
    digest = hashlib.sha256()
    try:
        server.call('POST', '/api/v1/tenants', {'operation_id': operation(), 'tenant_id': 'bench'}, MASTER)
        server.call('POST', '/api/v1/apikey', {'operation_id': operation(), 'key_id': 'bench',
                    'tenant_id': 'bench', 'apikey_base64': KEY}, MASTER)
        config = {'index_name': 'Issue 153', 'tokenizer': 'TantivySimpleV1', 'schema': [
            {'field': 'body', 'field_type': 'Text', 'store': True, 'index_lexical': True},
            {'field': 'priority', 'field_type': 'Int', 'store': True, 'filterable': True, 'index_lexical': False},
            {'field': 'vector', 'field_type': 'Json', 'store': True, 'index_lexical': False, 'index_vector': True}],
            'vector': {'dimensions': 2}}
        server.call('POST', '/api/v1/index', {'operation_id': operation(), 'collection_id': 'bench',
                    'embedding_space_id': 'issue153-fixed', 'config': config,
                    'capacity_profile': 'standard_10k' if n <= 10000 else 'bounded_100k'})
        for start in range(0, n, 64):
            rows = [corpus_row(i, n) for i in range(start, min(start+64, n))]
            for row in rows:
                digest.update(json.dumps(row, sort_keys=True).encode()+b'\n')
            writes = [{**row, 'operation_id': operation()} for row in rows]
            with (out/f'prepare-requests-{n}.jsonl').open('a') as saved:
                saved.write(json.dumps({'records': writes})+'\n')
            deadline = time.monotonic()+120
            pending = writes
            while pending:
                result, _ = server.call('POST', '/api/v1/index/bench/ingest', {'records': pending}, allowed=(200,429))
                if result.get('code') == 'busy' and time.monotonic() < deadline:
                    time.sleep(.1)
                    continue
                assert len(result['results']) == len(pending)
                retry = []
                for write, receipt in zip(pending, result['results']):
                    if receipt.get('code') == 'committed' and receipt.get('receipt'):
                        continue
                    if receipt.get('code') == 'not_attempted' and (result.get('error') or {}).get('code') == 'busy' and time.monotonic() < deadline:
                        retry.append(write)
                    else:
                        raise RuntimeError(('preparation write failed', receipt))
                pending = retry
                if pending:
                    print(f'prepare n={n}: retry {len(pending)} explicit Busy with original operation IDs', flush=True)
                    time.sleep(.1)
            if start % 1024 == 0:
                print(f'prepare n={n}: {start+len(rows)}', flush=True)
        deadline = time.monotonic()+120
        while True:
            try:
                checkpoint, _ = server.call('PATCH', '/api/v1/index/bench')
                break
            except RuntimeError as error:
                detail = error.args[0]
                if not (isinstance(detail, tuple) and detail[2] == 429 and
                        json.loads(detail[3]).get('code') == 'busy' and time.monotonic() < deadline):
                    raise
                time.sleep(.1)
        assert checkpoint['visible_seq'] == checkpoint['indexed_seq'], checkpoint
        result, _ = server.query({'query': 'common', 'length': 0, 'count': True})
        assert result['count_total'] == n, result
        dump(out/f'seed-{n}.json', {'n': n, 'sha256': digest.hexdigest(), 'config': config,
                                   'capacity_profile': 'standard_10k' if n <= 10000 else 'bounded_100k',
                                   'checkpoint': checkpoint, 'statistics': result['statistics']})
    finally:
        server.close()
    return seed


def workloads():
    return {
        'score': {'query': 'common', 'length': 32, 'count': True},
        'ties': {'query': 'tied', 'length': 32, 'count': True},
        'field_page': {'query': 'common', 'offset': 16, 'length': 16, 'count': True,
                       'sort': [{'field': 'priority', 'order': 'Descending'}]},
        'hybrid': {'query': 'common', 'query_vector': [1, 0], 'all': True,
                   'length': 16, 'count': True,
                   'rrf': {'constant': 60.0, 'keyword_candidates': 32, 'vector_candidates': 48}},
    }


def check_oracle(name, response):
    ids = lambda values: [f'r{i:08d}' for i in values]
    lexical = ids(range(1, 257, 8))
    expected = {'score': lexical, 'ties': ids(range(1, 33)),
                'field_page': ids(range(17, 33))}
    if name == 'hybrid':
        f32 = lambda x: struct.unpack('f', struct.pack('f', x))[0]
        scores = {}
        ranks = {}
        for channel, candidates in [('lexical', lexical), ('vector', ids(range(1, 49)))]:
            for rank, rid in enumerate(candidates):
                scores[rid] = f32(scores.get(rid, 0.0) + f32(1.0/(60.0+rank)))
                ranks.setdefault(rid, {})[channel] = rank
        expected[name] = sorted(scores, key=lambda rid: (-scores[rid], rid))[:16]
        for hit in response['hits']:
            rid = hit['record']['record_id']
            assert abs(hit['score']-scores[rid]) < 1e-8
            assert {c['channel']: c['rank'] for c in hit['channels']} == ranks[rid]
    assert [h['record']['record_id'] for h in response['hits']] == expected[name], (name, response)


def run_phase(server, query, expected, count, concurrency, output, metadata, allocation_file=None):
    def worker(_):
        value, latency = server.query(query)
        assert signature(value) == expected, ('response mismatch', metadata, value, expected)
        return latency, value['time']/1e6
    # Pool creation, connections and warmup precede both CPU and timing samples.
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(worker, range(24)))
        alloc0 = allocations(allocation_file) if allocation_file else None
        cpu0 = cpu(server.proc_pid)
        start = time.perf_counter()
        values = list(pool.map(worker, range(count)))
        wall = time.perf_counter()-start
        used_cpu = cpu(server.proc_pid)-cpu0
        alloc1 = allocations(allocation_file) if allocation_file else None
    summary = {**metadata, 'requests': count, 'errors': 0, 'wall_s': wall,
               'cpu_s': used_cpu, 'cpu_ms_per_request': used_cpu*1000/count,
               'qps': count/wall, **quantiles([v[0] for v in values]),
               'server_latency': quantiles([v[1] for v in values])}
    if alloc0:
        assert alloc1[0] > alloc0[0] and alloc1[1] > alloc0[1], 'allocation counters did not advance'
        summary['allocation_counters_before'] = alloc0
        summary['allocation_counters_after'] = alloc1
        summary['allocation_calls_per_request'] = (alloc1[0]-alloc0[0])/count
        summary['requested_allocation_bytes_per_request'] = (alloc1[1]-alloc0[1])/count
    for latency, server_latency in values:
        output.write(json.dumps({**metadata, 'ms': latency, 'server_ms': server_latency})+'\n')
    output.flush()
    print(json.dumps(summary), flush=True)
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--before', type=Path, required=True)
    ap.add_argument('--after', type=Path, required=True)
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--sizes', type=int, nargs='+', default=[10000, 20000])
    ap.add_argument('--rounds', type=int, default=4)
    ap.add_argument('--requests', type=int, default=200)
    ap.add_argument('--allocation', action='store_true')
    ap.add_argument('--seed-root', type=Path)
    args = ap.parse_args()
    if not __debug__:
        ap.error('correctness checks require assertions; do not use python -O')
    if args.rounds <= 0 or args.requests <= 0 or len(set(args.sizes)) != len(args.sizes):
        ap.error('rounds/requests must be positive and sizes must be unique')
    if any(n < 256 or n % 8 for n in args.sizes):
        ap.error('sizes must be >= 256 and divisible by 8')
    if len(os.sched_getaffinity(0)) < 3:
        ap.error('at least three available logical CPUs are required')
    out = args.out.resolve()
    # Refuse even an empty existing directory before touching any evidence.
    out.mkdir(parents=True, exist_ok=False)
    available_cpus = sorted(os.sched_getaffinity(0))
    cpus = available_cpus[:2]
    if len(available_cpus) > 2:
        os.sched_setaffinity(0, available_cpus[2:])
    binaries = {'before': args.before.resolve(), 'after': args.after.resolve()}
    binary_hashes = {k: hashlib.file_digest(p.open('rb'), 'sha256').hexdigest() for k,p in binaries.items()}
    assert len(set(binary_hashes.values())) == 2, 'refuse comparison of identical binaries'
    dump(out/'environment.json', {'cpus': cpus, 'uname': list(os.uname()),
         'cpu_max': Path('/sys/fs/cgroup/cpu.max').read_text(),
         'memory_max': Path('/sys/fs/cgroup/memory.max').read_text(),
         'binary_sha256': binary_hashes,
         'rounds': args.rounds, 'requests_per_phase': args.requests, 'workloads': workloads(),
         'mode': 'allocation-instrumented' if args.allocation else 'uninstrumented closed-loop HTTP'})
    summaries = []
    with (out/'requests.jsonl').open('w') as output:
        for n in args.sizes:
            assert n >= 256 and n % 8 == 0, 'fixed score oracle requires n >= 256 and divisible by 8'
            if args.seed_root:
                seed = args.seed_root.resolve()/f'seed-{n}'
                assert seed.is_dir() and (args.seed_root/f'seed-{n}.json').exists()
                shutil.copyfile(args.seed_root/f'seed-{n}.json', out/f'seed-{n}.json')
            else:
                seed = prepare(binaries['after'], out, n, cpus)
            expected = {}
            for round_id in range(args.rounds):
                order = ['before', 'after'] if round_id % 2 == 0 else ['after', 'before']
                for variant in order:
                    data = out/f'run-{n}-{round_id}-{variant}-{uuid.uuid4().hex}'
                    # Atomic-write staging files are not referenced by the committed
                    # Tantivy manifest; managed mounts can retain stale directory entries.
                    shutil.copytree(seed, data, dirs_exist_ok=True, ignore=lambda directory, names: [
                        name for name in names if re.fullmatch(r'\.[0-9a-f]{32}\..+\.[A-Za-z0-9]{6}', name)])
                    alloc_file = out/'alloc.bin' if args.allocation else None
                    server = Server(binaries[variant], data, out/f'{n}-{round_id}-{variant}.log', cpus, alloc_file)
                    try:
                        for name, query in workloads().items():
                            observed = signature(server.query(query)[0])
                            check_oracle(name, observed)
                            if name not in expected:
                                expected[name] = observed
                            assert observed == expected[name], ('baseline mismatch', n, name, variant)
                            assert observed['count_total'] == n
                            for concurrency in ([1] if args.allocation else [1, 4]):
                                meta = {'n': n, 'round': round_id, 'variant': variant,
                                        'workload': name, 'concurrency': concurrency}
                                summaries.append(run_phase(server, query, expected[name], args.requests,
                                                           concurrency, output, meta, alloc_file))
                                dump(out/'summary.json', summaries)
                        dump(out/f'expected-{n}.json', expected)
                    finally:
                        server.close()
                        shutil.rmtree(data)


if __name__ == '__main__':
    main()
