"""Manual Linux ABBA comparison on private copies of one seed per workload."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--before', type=Path, required=True)
parser.add_argument('--after', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--cpus', default='6,7')
parser.add_argument('--workloads', default='1000:true,2000:true,8000:false')
parser.add_argument('--mixed', choices=['lexical', 'facet', 'hybrid'])
args = parser.parse_args()
os.sched_setaffinity(0, {int(cpu) for cpu in args.cpus.split(',')})
root = args.output.resolve()
root.mkdir(parents=True, exist_ok=True)
binaries = {'base': args.before.resolve(), 'candidate': args.after.resolve()}
metadata = {'binaries': {name: hashlib.sha256(path.read_bytes()).hexdigest()
                         for name, path in binaries.items()}, 'cpus': args.cpus,
            'mixed': args.mixed, 'runs': []}

def invoke(binary, data, n, ttl, output, mixed=None):
    env = dict(os.environ)
    env.pop('ANYSEEK_QUERY_PROBE_MIXED', None)
    if mixed:
        env['ANYSEEK_QUERY_PROBE_MIXED'] = mixed
    with output.with_suffix('.log').open('w') as log:
        subprocess.run(['python3', str(Path(__file__).with_name('resource_run.py')), str(output.with_suffix('.resources')), str(binary), str(data), str(n), str(ttl).lower(), str(output)],
                       stdout=log, stderr=log, env=env, check=True)

def tree_hash(path):
    digest = hashlib.sha256()
    for file in sorted(path.rglob('*')):
        if file.is_file():
            digest.update(str(file.relative_to(path)).encode())
            digest.update(hashlib.sha256(file.read_bytes()).digest())
    return digest.hexdigest()

for workload in args.workloads.split(','):
    population, ttl_text = workload.split(':')
    assert ttl_text in ('true', 'false')
    n, ttl = int(population), ttl_text == 'true'
    assert 32 <= n <= 10000
    fixture = root / f'seed-{n}-{ttl}'
    if not fixture.exists():
        invoke(binaries['base'], fixture, n, ttl, root / f'seed-{n}-{ttl}.json')
    seed_hash = tree_hash(fixture)
    expected = None
    for i, version in enumerate(['base', 'candidate', 'candidate', 'base'] * 3):
        data = root / f'run-{n}-{ttl}-{i}'
        shutil.copytree(fixture, data)  # Refuse to overwrite another run's data.
        assert tree_hash(data) == seed_hash
        output = root / f'result-{n}-{ttl}-{i}.json'
        start = time.monotonic()
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        invoke(binaries[version], data, n, ttl, output, args.mixed)
        end = resource.getrusage(resource.RUSAGE_CHILDREN)
        profile = json.loads(output.read_text())
        signature = [row['result'] for row in profile['rows']]
        if expected is None:
            expected = signature
        assert expected == signature, 'ID/score/channel/count parity failure'
        metadata['runs'].append({
            'n': n, 'ttl': ttl, 'version': version, 'run': i, 'seed_sha256': seed_hash,
            'wall_s': time.monotonic() - start,
            'cpu_s': end.ru_utime + end.ru_stime - usage.ru_utime - usage.ru_stime,
            'profile': profile,
        })
        (root / 'results.json').write_text(json.dumps(metadata, separators=(',', ':')) + '\n')
        shutil.rmtree(data)
        print(n, ttl, i, version, 'passed', flush=True)
print('PASS exact IDs, score bits, channel ranks and counts across each ABBA group')
