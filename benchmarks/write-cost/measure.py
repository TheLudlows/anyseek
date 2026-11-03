#!/usr/bin/env python3
"""Write-cost paired HTTP measurement. Server/proc helpers adapted from PR190
b531aa60f770ffe1155a6dcb5f6f96eaaacb2c25; allocation counter reused unchanged.

Only disposable local servers are supported. No running user server is touched.
Latency includes HTTP I/O, excludes response comparison; CPU is server process
user+system time from /proc, divided by successful requests. Closed-loop load.
"""
import argparse
import fcntl
import resource as usage
import gzip
import base64
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import threading
import time
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'capacity'))
from async_compare import summarize_maintenance

SECRET = 'disposable-issue153-benchmark'
MASTER = base64.b64encode(hashlib.sha256(SECRET.encode()).digest()).decode()
KEY = base64.b64encode(hashlib.sha256(b'issue153-tenant').digest()).decode()
HERE = Path(__file__).resolve().parent


def dump(path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + '\n')


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


class Server:
    def __init__(self, binary, data, log, cpus, allocation_file=None):
        self.child_usage_before = usage.getrusage(usage.RUSAGE_CHILDREN)
        self.local = threading.local()
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]
        temporary = data.parent/'tmp'
        temporary.mkdir(exist_ok=True)
        env = {**os.environ, 'TMPDIR': str(temporary), 'MASTER_KEY_SECRET': SECRET,
               'TOKIO_WORKER_THREADS': '2', 'RAYON_NUM_THREADS': '2', 'RUST_LOG': 'anyseek::record::maintenance=info', 'ANYSEEK_STORAGE_BACKEND': 'local'}
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
        started = time.perf_counter()
        if self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait()
                raise RuntimeError("shutdown timed out")
        self.log.close()
        assert self.p.returncode == 0, self.p.returncode
        used = usage.getrusage(usage.RUSAGE_CHILDREN)
        self.total_cpu_s = used.ru_utime+used.ru_stime-self.child_usage_before.ru_utime-self.child_usage_before.ru_stime
        self.shutdown_ms = (time.perf_counter()-started)*1000

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
        self.last_status = response.status
        return json.loads(raw), elapsed

    def query(self, query):
        return self.call('POST', '/api/v1/index/bench/query', query)


PATH = '/api/v1/index/bench'
V1 = [1.0] + [0.0]*31
V2 = [0.0, 1.0] + [0.0]*30


def digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def oid(epoch, nonce):
    return {'epoch': epoch, 'nonce': nonce}


def resource(server):
    fields = dict(line.split(':', 1) for line in Path(f'/proc/{server.proc_pid}/status').read_text().splitlines() if ':' in line)
    return {'cpu_s': cpu(server.proc_pid), 'rss_kib': int(fields['VmRSS'].split()[0]),
            'hwm_kib': int(fields['VmHWM'].split()[0])}


def request(server, method, path, body=None):
    """Bounded explicit Busy retries only; same object/IDs, all attempts retained."""
    started = time.perf_counter()
    attempts = []
    known = {}
    for _ in range(1200):
        result, elapsed = server.call(method, path, body, allowed=(200, 429))
        items = result.get('results', [])
        busy_items = sum(row.get('code') == 'busy' for row in items)
        attempts.append({'status': server.last_status, 'http_ms': elapsed, 'response': result, 'busy_items': busy_items})
        for row in items:
            if row.get('receipt'):
                key = digest(row['operation_id'])
                assert key not in known or known[key] == row['receipt'], ('replayed receipt changed', row)
                known[key] = row['receipt']
        code = result.get('code', (result.get('error') or {}).get('code'))
        retryable_items = all(row.get('code') in ('applied', 'committed', 'busy', 'not_attempted') for row in items)
        retryable = (code == 'busy' or busy_items > 0) and retryable_items
        if not retryable:
            return result, (time.perf_counter()-started)*1000, attempts
        if time.perf_counter()-started > 120:
            break
        time.sleep(.01)
    raise RuntimeError(('Busy deadline', attempts))


def wait_cut(server, cut):
    started = time.perf_counter()
    polls = 0
    while time.perf_counter()-started < 120:
        p, _, _ = request(server, 'GET', PATH+'/ingest')
        polls += 1
        assert not p['recovery_required'] and not p['index_error'], p
        if min(p['lexical_seq'], p['vector_seq']) >= cut:
            return (time.perf_counter()-started)*1000, polls, p
        time.sleep(.002)
    raise TimeoutError(('index catch-up', cut, p))


class WriteFailure(RuntimeError):
    def __init__(self, rows, result, attempts, elapsed):
        super().__init__('non-successful write; retain and recover separately')
        self.evidence = {'commands': rows, 'response': result, 'attempts': attempts, 'ack_ms': elapsed}


def write(server, endpoint, rows):
    result, elapsed, attempts = request(server, 'POST', PATH+'/'+endpoint,
                                       {'writes' if endpoint == 'batch' else 'records': rows})
    expected = 'applied' if endpoint == 'batch' else 'committed'
    assert len(result['results']) == len(rows), result
    if not all(r['code'] == expected and r.get('receipt') for r in result['results']):
        raise WriteFailure(rows, result, attempts, elapsed)
    receipts = [r['receipt'] for r in result['results']]
    for row, receipt in zip(rows, receipts):
        assert receipt['record_id'] == row['record_id'], (row, receipt)
    catchup, polls, progress = wait_cut(server, max(r['seq'] for r in receipts))
    return {'ack_ms': elapsed, 'catchup_ms': catchup, 'complete_ms': elapsed+catchup,
            'polls': polls, 'busy': len(attempts)-1, 'busy_items': sum(a['busy_items'] for a in attempts), 'attempts': attempts,
            'receipts': receipts, 'progress': progress}


def files(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob('*')) if p.is_file()}


def prepare(binary, out, n, cpus, epoch):
    root = out/f'seed-{n}'
    if (out/f'seed-{n}.json').exists():
        info = json.loads((out/f'seed-{n}.json').read_text())
        assert files(root) == info['files'] and info['epoch'] == epoch
        return root
    server = Server(binary, root, out/f'prepare-{n}.log', cpus)
    corpus = hashlib.sha256()
    try:
        server.call('POST', '/api/v1/tenants', {'operation_id': oid(epoch, 'tenant'), 'tenant_id': 'bench'}, MASTER)
        server.call('POST', '/api/v1/apikey', {'operation_id': oid(epoch, 'key'), 'key_id': 'bench',
                    'tenant_id': 'bench', 'apikey_base64': KEY}, MASTER)
        config = {'index_name': 'write cost', 'tokenizer': 'TantivySimpleV1', 'schema': [
            {'field': 'body', 'field_type': 'Text', 'store': True, 'index_lexical': True},
            {'field': 'group', 'field_type': 'Int', 'store': True, 'filterable': True, 'index_lexical': False},
            {'field': 'vector', 'field_type': 'Json', 'store': True, 'index_lexical': False, 'index_vector': True}],
            'vector': {'dimensions': 32}}
        server.call('POST', '/api/v1/index', {'operation_id': oid(epoch, 'collection'), 'collection_id': 'bench',
                    'config': config, 'capacity_profile': 'standard_10k'})
        for start in range(0, n, 64):
            rows = [{'operation_id': oid(epoch, f'seed-{i}'), 'record_id': f'r{i:06d}', 'expected': 'Absent',
                     'document': {'body': 'seedword '+('padding '*30), 'group': 0, 'vector': V1}}
                    for i in range(start, min(n, start+64))]
            corpus.update(json.dumps(rows, sort_keys=True).encode())
            write(server, 'ingest', rows)
        request(server, 'PATCH', PATH)
        info = server.call('GET', PATH)[0]
        assert info['statistics']['active_records'] == n, info
    finally:
        server.close()
    dump(out/f'seed-{n}.json', {'epoch': epoch, 'n': n, 'config': config, 'info': info,
                              'corpus_sha256': corpus.hexdigest(), 'files': files(root)})
    return root


def commands(epoch, kind, start, batch):
    rows = []
    for i in range(start, start+batch):
        row = {'operation_id': oid(epoch, f'{kind}-{i:06d}'), 'record_id': f'm{i:06d}'}
        if kind == 'insert':
            row.update(expected='Absent', document={'body': 'insertword '+('padding '*30), 'group': 1})
        elif kind == 'patch':
            row.update(expected={'Match': 1}, vector_patch={'vector': V1})
        elif kind == 'update':
            row.update(expected={'Match': 1}, document={'body': 'updateword '+('padding '*30), 'group': 1, 'vector': V2})
        else:
            row.update(expected={'Match': 2}, document=None)
        rows.append(row)
    return rows


def verify(server, kind, count, n, extra=0):
    expected = {f'm{i:06d}' for i in range(count)} if kind != 'delete' else set()
    token = 'insertword' if kind in ('insert', 'patch') else 'updateword'
    result = server.query({'query': token, 'length': 1000, 'count': True})[0]
    actual = {h['record']['record_id'] for h in result['hits']}
    assert actual == expected and result['count_total'] == len(expected), (kind, len(actual), len(expected), result)
    old = 'updateword' if token == 'insertword' else 'insertword'
    assert server.query({'query': old, 'length': 0, 'count': True})[0]['count_total'] == 0
    vector = server.query({'query_vector': V2 if kind == 'update' else V1, 'all': True, 'length': 1000,
                           'filter': [{'Int': {'field': 'group', 'filter': {'start': 1, 'end': 2}}}]})[0]
    vector_expected = expected if kind in ('patch', 'update') else set()
    assert {h['record']['record_id'] for h in vector['hits']} == vector_expected, (kind, vector)
    assert server.query({'query': '', 'length': 0, 'count': True})[0]['count_total'] == n+len(expected)+extra
    if expected:
        row = server.call('GET', PATH+'/doc/m000000')[0]
        assert row['version'] == (2 if kind == 'update' else 1), row
        status = server.call('GET', PATH+'/doc/m000000/status')[0]
        assert status['pending_vector_fields'] == (['vector'] if kind == 'insert' else []), status
    return {'lexical_ids_sha256': digest(sorted(actual)), 'vector_ids_sha256': digest(sorted(vector_expected)),
            'active_records': n+len(expected)+extra, 'checked': True}


def cache_dirs(root):
    # Filesystem evidence of a newly built cache, not a timer for rotate_tail.
    return {str(p.relative_to(root)) for p in root.glob('collections/*/*/*/cache-*') if p.is_dir()}


def run(binary, label, seed, out, n, endpoint, batch, rep, cpus, epoch, alloc, short):
    name = f'{label}-{n}-{endpoint}-b{batch}-r{rep}'
    folder = out/name
    folder.mkdir()
    root = folder/'db'
    shutil.copytree(seed, root)
    expected_seed = json.loads((out/f'seed-{n}.json').read_text())['files']
    assert files(root) == expected_seed, 'seed copy differs'
    afile = folder/'alloc.bin' if alloc else None
    server = Server(binary, root, folder/'server.log', cpus, afile)
    record = {'run': name, 'version': label, 'n': n, 'endpoint': endpoint, 'batch': batch, 'rep': rep,
              'allocation_run': alloc, 'harness_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'seed_files_sha256': digest(expected_seed), 'groups': [], 'status': 'running'}
    history = []
    count_calls = 4 if short else (128 if batch == 1 else 40)
    total = (5+count_calls)*batch
    try:
        overall = resource(server)
        workflow_start = time.perf_counter()
        with gzip.open(folder/'requests.jsonl.gz', 'wt') as raw:
            for kind in ('insert', 'patch', 'update', 'delete'):
                samples = []
                for call in range(5+count_calls):
                    if call == 5:
                        before = resource(server)
                        allocated = allocations(afile) if afile else None
                        group_start = time.perf_counter()
                    exclusive_process_check(server.proc_pid)
                    free_bytes = shutil.disk_usage(root).free
                    assert free_bytes >= 512*1024**2, ('filesystem reserve below 512MiB', free_bytes)
                    rows = commands(epoch, kind, call*batch, batch)
                    dirs = cache_dirs(root)
                    measurement = write(server, endpoint, rows)
                    measurement.update(kind=kind, call=call, warmup=call < 5, fs_free_bytes=free_bytes, request_sha256=digest(rows),
                                       new_cache_dirs=sorted(cache_dirs(root)-dirs))
                    versions = [r['version'] for r in measurement['receipts']]
                    assert versions == [1 if kind in ('insert','patch') else 2 if kind == 'update' else 3]*batch, versions
                    history.extend(zip(rows, measurement['receipts']))
                    raw.write(json.dumps(measurement, separators=(',', ':'))+'\n')
                    raw.flush()
                    if call >= 5:
                        samples.append(measurement)
                elapsed = time.perf_counter()-group_start
                after = resource(server)
                allocation = None if not afile else [b-a for a,b in zip(allocated, allocations(afile))]
                group = {'kind': kind, 'calls': count_calls, 'records': count_calls*batch,
                         'wall_s': elapsed, 'wall_records_s': count_calls*batch/elapsed, 'server_cpu_s': after['cpu_s']-before['cpu_s'],
                         'rss_before_kib': before['rss_kib'], 'rss_after_kib': after['rss_kib'],
                         'lifetime_hwm_kib': after['hwm_kib'], 'allocation_calls_bytes': allocation,
                         'busy': sum(s['busy'] for s in samples), 'busy_items': sum(s['busy_items'] for s in samples),
                         'cache_creation_calls': sum(bool(s['new_cache_dirs']) for s in samples)}
                for metric in ('ack_ms', 'catchup_ms', 'complete_ms'):
                    values = [s[metric] for s in samples]
                    group[metric] = {**quantiles(values), 'max_ms': max(values), 'sum_ms': sum(values)}
                group['completed_records_s'] = group['records']/(group['complete_ms']['sum_ms']/1000)
                group['validation'] = verify(server, kind, total, n)
                record['groups'].append(group)
                dump(folder/'summary.json', record)
                print(name, kind, round(group['ack_ms']['p95_ms'], 3), round(group['completed_records_s'], 1), flush=True)
        record['workflow_cpu_s'] = resource(server)['cpu_s']-overall['cpu_s']
        record['workflow_wall_s'] = time.perf_counter()-workflow_start
        # Preserve live representatives as well as tombstones for reopen validation.
        witnesses = [{'operation_id': oid(epoch, f'witness-{i}'), 'record_id': f'w{i}',
                      'expected': 'Absent', 'document': {'body': 'reopenword', 'group': 2}} for i in range(3)]
        witnesses += [
            {'operation_id': oid(epoch, 'witness-patch'), 'record_id': 'w1', 'expected': {'Match': 1}, 'vector_patch': {'vector': V1}},
            {'operation_id': oid(epoch, 'witness-update'), 'record_id': 'w2', 'expected': {'Match': 1},
             'document': {'body': 'reopenword updated', 'group': 2, 'vector': V2}}]
        witness_result = write(server, 'batch', witnesses)
        history.extend(zip(witnesses, witness_result['receipts']))
        record['witness_receipts'] = witness_result['receipts']
        record['witness_records'] = [server.call('GET', PATH+f'/doc/w{i}')[0] for i in range(3)]
        # Receipt reads and full original-ID replay are outside all measured windows.
        for row, receipt in history[::max(1, len(history)//16)]:
            op = row['operation_id']
            got = server.call('GET', PATH+f"/operation/{op['epoch']}/{op['nonce']}")[0]
            assert got['receipt'] == receipt, got
        server.close()
        record['process_cpu_including_startup_validation_shutdown_s'] = server.total_cpu_s
        record['shutdown_ms'] = server.shutdown_ms
        server = None
        started = time.perf_counter()
        server = Server(binary, root, folder/'reopen.log', cpus)
        record['reopen_ready_ms'] = (time.perf_counter()-started)*1000
        record['reopen_validation'] = verify(server, 'delete', total, n, extra=3)
        assert [server.call('GET', PATH+f'/doc/w{i}')[0] for i in range(3)] == record['witness_records']
        for i in range(3):
            pending = server.call('GET', PATH+f'/doc/w{i}/status')[0]['pending_vector_fields']
            assert pending == (['vector'] if i == 0 else []), pending
        found = server.query({'query': 'reopenword', 'length': 10})[0]
        assert {h['record']['record_id'] for h in found['hits']} == {'w0','w1','w2'}
        found = server.query({'query_vector': V1, 'all': True, 'length': 10,
                              'filter': [{'Int': {'field': 'group', 'filter': {'start': 2, 'end': 3}}}]})[0]
        assert {h['record']['record_id'] for h in found['hits']} == {'w1','w2'}
        for start in range(0, len(history), 64):
            part = history[start:start+64]
            replay = write(server, 'batch', [row for row, _ in part])
            assert replay['receipts'] == [receipt for _, receipt in part]
        record['replayed_receipts'] = len(history)
        record['final_progress'] = server.call('GET', PATH+'/ingest')[0]
        assert record['final_progress']['committed_seq'] == max(receipt['seq'] for _, receipt in history)
        record['maintenance'] = summarize_maintenance((folder/'server.log').read_text())
        record['status'] = 'passed'
    except WriteFailure as error:
        record['status'] = 'failed'
        record['error'] = repr(error)
        record['failure'] = error.evidence
        print(name, 'FAILED; retained non-success response', flush=True)
    except BaseException as error:
        record['status'] = 'driver_failed'
        record['error'] = repr(error)
        raise
    finally:
        if server is not None:
            server.close()
        dump(folder/'summary.json', record)
    # Keep small evidence; databases and binaries never enter git.
    if record['status'] == 'passed':
        shutil.rmtree(root)


def exclusive_process_check(exclude=None):
    conflicts = []
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            argv = (proc/'cmdline').read_bytes().split(b'\0')
            command = Path(argv[0].decode()).name
            if int(proc.name) != exclude and (command in ('cargo', 'rustc', 'anyseek_server') or
                    any(arg.startswith(b'index_path=') for arg in argv)):
                conflicts.append((proc.name, command, str((proc/'cwd').resolve())))
        except (OSError, ValueError):
            pass
    assert not conflicts, ('other build/benchmark process; stop and reschedule', conflicts)
    return {'visible_conflicts': conflicts, 'checked_at': time.time()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', action='append', required=True, help='label=/absolute/binary')
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--sizes', default='1000,5000')
    parser.add_argument('--batches', default='1,16')
    parser.add_argument('--cpus', default='0,1')
    parser.add_argument('--client-cpus', default='2,3')
    parser.add_argument('--wait-idle-seconds', type=int, default=0)
    parser.add_argument('--resume', action='store_true', help='continue untouched planned runs; never replace an existing result')
    parser.add_argument('--allocation', action='store_true')
    parser.add_argument('--short', action='store_true', help='smoke only, never performance evidence')
    args = parser.parse_args()
    os.sched_setaffinity(0, set(map(int, args.client_cpus.split(','))))
    binaries = dict(item.split('=', 1) for item in args.binary)
    binaries = {k: Path(v).resolve() for k,v in binaries.items()}
    if 'before' in binaries and 'after' in binaries:
        assert hashlib.sha256(binaries['before'].read_bytes()).digest() != hashlib.sha256(binaries['after'].read_bytes()).digest(), 'identical before/after binaries: check stale shared Cargo target'
    labels = list(binaries)
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=args.resume)
    with open('/tmp/anyseek-performance.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        deadline = time.monotonic()+args.wait_idle_seconds
        while True:
            try:
                guard = exclusive_process_check()
                break
            except AssertionError as error:
                if time.monotonic() >= deadline:
                    raise
                print('waiting for exclusive resources:', error, flush=True)
                time.sleep(5)
        cpus = list(map(int, args.cpus.split(',')))
        epoch = int(time.time()//86400)
        if args.resume:
            prior = json.loads((args.out/'environment.json').read_text())
            assert prior['epoch'] == epoch and prior['orders'] == [labels, labels[::-1]]
            for key in ('sizes', 'batches', 'cpus', 'client_cpus', 'allocation', 'short'):
                assert prior['argv'][key] == getattr(args, key), key
            assert all(prior['binaries'][k]['sha256'] == hashlib.sha256(v.read_bytes()).hexdigest() for k,v in binaries.items())
        dump(args.out/('resume-environment.json' if args.resume else 'environment.json'), {'epoch': epoch, 'argv': vars(args) | {'out': str(args.out)},
             'cpu_max': Path('/sys/fs/cgroup/cpu.max').read_text(),
             'memory_max': Path('/sys/fs/cgroup/memory.max').read_text(),
             'filesystem_type': subprocess.check_output(['stat', '-f', '-c', '%T', str(args.out)], text=True).strip(),
             'uname': list(os.uname()), 'cpuinfo': Path('/proc/cpuinfo').read_text().split('\n\n')[0],
             'guard': guard, 'orders': [labels, labels[::-1]],
             'binaries': {k: {'path': str(v), 'sha256': hashlib.sha256(v.read_bytes()).hexdigest()} for k,v in binaries.items()}})
        for n in map(int, args.sizes.split(',')):
            planned = [args.out/f'{label}-{n}-{endpoint}-b{batch}-r{rep}'
                       for batch in map(int, args.batches.split(',')) for endpoint in ('batch','ingest')
                       for rep in range(2) for label in labels]
            if args.resume and all((p/'summary.json').exists() for p in planned):
                assert all(json.loads((p/'summary.json').read_text())['status'] in ('passed', 'failed') for p in planned)
                continue
            seed = prepare(binaries[labels[0]], args.out, n, cpus, epoch)
            for batch in map(int, args.batches.split(',')):
                for endpoint in ('batch', 'ingest'):
                    for rep, order in enumerate([labels, labels[::-1]]):
                        for label in order:
                            existing = args.out/f'{label}-{n}-{endpoint}-b{batch}-r{rep}'
                            if args.resume and existing.exists():
                                old = json.loads((existing/'summary.json').read_text())
                                assert old['status'] in ('passed', 'failed'), old['status']
                                continue
                            exclusive_process_check()
                            run(binaries[label], label, seed, args.out, n, endpoint, batch, rep, cpus,
                                epoch, args.allocation, args.short)
            shutil.rmtree(seed)


if __name__ == '__main__':
    main()
