#!/usr/bin/env python3
"""Summarize complete paired rounds; never pool different workloads/concurrency."""
import csv
import argparse
import json
from pathlib import Path
import statistics
import collections
import gzip
import hashlib
import itertools
import math
import sys

from measure import check_oracle, corpus_row, quantiles, workloads


def read_json(path):
    if path.exists():
        return json.loads(path.read_text())
    return json.loads(gzip.decompress(path.with_suffix(path.suffix+'.gz').read_bytes()))


def phase(row):
    return tuple(row[k] for k in ('n', 'round', 'variant', 'workload', 'concurrency'))


def verify(root, rows, alloc):
    # The manifest detects archive damage; it is not a signed attestation.
    manifest = root/'SHA256SUMS.json'
    if manifest.exists():
        hashes = read_json(manifest)
        files = {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and p != manifest}
        assert files == set(hashes), 'archive manifest membership mismatch'
        for name, expected in hashes.items():
            assert hashlib.sha256((root/name).read_bytes()).hexdigest() == expected, name
    provenance = read_json(root/'build-provenance.json') if (root/'build-provenance.json').exists() else None
    binary_hashes = None
    seeds = {}
    expected_results = {}
    for lane, summaries, rounds, concurrency in [('measured', rows, 4, [1, 4]), ('allocation', alloc, 2, [1])]:
        directory = root/lane
        env = read_json(directory/'environment.json')
        assert env['rounds'] == rounds and env['requests_per_phase'] > 0
        assert env['workloads'] == workloads(), lane
        assert len(set(env['binary_sha256'].values())) == 2
        if binary_hashes is None:
            binary_hashes = env['binary_sha256']
        assert env['binary_sha256'] == binary_hashes, 'different binaries in allocation run'
        if provenance:
            assert binary_hashes == {k: v['binary_sha256'] for k, v in provenance['versions'].items()}
        wanted = set(itertools.product([10000, 20000], range(rounds), ['before', 'after'], workloads(), concurrency))
        assert collections.Counter(map(phase, summaries)) == collections.Counter({k: 1 for k in wanted}), lane
        samples = collections.defaultdict(list)
        request_file = directory/'requests.jsonl'
        opener = request_file.open if request_file.exists() else request_file.with_suffix('.jsonl.gz').open
        with opener('rb') as stream:
            # gzip.open also accepts the already-open stream.
            lines = stream if request_file.exists() else gzip.GzipFile(fileobj=stream)
            for line in lines:
                row = json.loads(line)
                assert all(math.isfinite(row[k]) and row[k] >= 0 for k in ('ms', 'server_ms'))
                samples[phase(row)].append(row)
        assert set(samples) == wanted, 'unexpected or missing request phases'
        for row in summaries:
            values = samples[phase(row)]
            count = row['requests']
            assert count == env['requests_per_phase'] == len(values) and row['errors'] == 0
            for key, value in quantiles([v['ms'] for v in values]).items():
                assert row[key] == value, (phase(row), key)
            assert row['server_latency'] == quantiles([v['server_ms'] for v in values])
            assert row['wall_s'] > 0 and row['cpu_s'] >= 0
            assert math.isclose(row['cpu_ms_per_request'], row['cpu_s']*1000/count)
            assert math.isclose(row['qps'], count/row['wall_s'])
            if lane == 'allocation':
                for index, metric in enumerate(['allocation_calls_per_request', 'requested_allocation_bytes_per_request']):
                    assert math.isfinite(row[metric]) and row[metric] > 0
                    if 'allocation_counters_before' in row:
                        delta = row['allocation_counters_after'][index]-row['allocation_counters_before'][index]
                        assert delta > 0 and math.isclose(row[metric], delta/count)
        for n in [10000, 20000]:
            seed = read_json(directory/f'seed-{n}.json')
            digest = hashlib.sha256()
            for i in range(n):
                digest.update(json.dumps(corpus_row(i, n), sort_keys=True).encode()+b'\n')
            assert seed['n'] == n and seed['sha256'] == digest.hexdigest(), (lane, n)
            assert seed['checkpoint']['visible_seq'] == seed['checkpoint']['indexed_seq'] == n
            if n in seeds:
                assert seed == seeds[n], 'allocation seed differs'
            seeds[n] = seed
            responses = read_json(directory/f'expected-{n}.json')
            assert set(responses) == set(workloads())
            for name, response in responses.items():
                check_oracle(name, response)
                assert response['count_total'] == response['visible_seq'] == n
                assert response['count'] == len(response['hits'])
                for hit in response['hits']:
                    record = hit['record']
                    i = n-int(record['record_id'][1:])
                    assert record['document'] == corpus_row(i, n)['document']
            if n in expected_results:
                assert responses == expected_results[n], 'allocation oracle differs'
            expected_results[n] = responses
    legacy = sum('allocation_counters_before' not in row for row in alloc)
    print(f'Verified {sum(r["requests"] for r in rows)} timing / '
          f'{sum(r["requests"] for r in alloc)} allocation samples; '
          f'{legacy} allocation phases lack historical counter snapshots.', file=sys.stderr)

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('input', type=Path, help='Directory containing measured/ and allocation/')
parser.add_argument('--out', type=Path, required=True, help='Directory for generated CSV files')
args = parser.parse_args()
if not __debug__:
    parser.error('verification requires assertions; do not use python -O')
ROOT = args.input.resolve()
OUT = args.out.resolve()
rows = json.loads((ROOT/'measured/summary.json').read_text())
alloc = json.loads((ROOT/'allocation/summary.json').read_text())
assert OUT != ROOT and ROOT not in OUT.parents, 'write summaries outside source evidence'
verify(ROOT, rows, alloc)
OUT.mkdir(parents=True, exist_ok=True)
metrics = ['cpu_ms_per_request', 'p95_ms', 'p99_ms']
table = []
for n in sorted({r['n'] for r in rows}):
    for workload in ['score', 'ties', 'field_page', 'hybrid']:
        for concurrency in [1,4]:
            selected = [r for r in rows if (r['n'],r['workload'],r['concurrency']) == (n,workload,concurrency)]
            before = sorted([r for r in selected if r['variant']=='before'], key=lambda r:r['round'])
            after = sorted([r for r in selected if r['variant']=='after'], key=lambda r:r['round'])
            assert [r['round'] for r in before] == [0,1,2,3]
            assert [r['round'] for r in after] == [0,1,2,3]
            item = {'n':n, 'workload':workload, 'concurrency':concurrency}
            for metric in metrics:
                b=statistics.median(r[metric] for r in before)
                a=statistics.median(r[metric] for r in after)
                item.update({metric+'_before':b, metric+'_after':a, metric+'_change_pct':100*(a/b-1)})
                item[metric+'_paired_changes_pct'] = [100*(a[metric]/b[metric]-1) for b,a in zip(before,after)]
            table.append(item)
with (OUT/'comparison.csv').open('w') as f:
    writer=csv.DictWriter(f,fieldnames=table[0].keys(),lineterminator='\n')
    writer.writeheader();writer.writerows(table)
allocation_table=[]
for n in sorted({r['n'] for r in alloc}):
    for workload in ['score','ties','field_page','hybrid']:
        selected=[r for r in alloc if (r['n'],r['workload'])==(n,workload)]
        row={'n':n,'workload':workload}
        for metric in ['allocation_calls_per_request','requested_allocation_bytes_per_request']:
            b=statistics.median(r[metric] for r in selected if r['variant']=='before')
            a=statistics.median(r[metric] for r in selected if r['variant']=='after')
            row.update({metric+'_before':b,metric+'_after':a,metric+'_change_pct':100*(a/b-1)})
        allocation_table.append(row)
with (OUT/'allocation-comparison.csv').open('w') as f:
    writer=csv.DictWriter(f,fieldnames=allocation_table[0].keys(),lineterminator='\n')
    writer.writeheader();writer.writerows(allocation_table)

print(json.dumps({'timing': table, 'allocations': allocation_table}, ensure_ascii=False, indent=2))
