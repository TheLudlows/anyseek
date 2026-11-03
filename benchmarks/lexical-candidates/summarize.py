#!/usr/bin/env python3
"""Summarize complete paired rounds; never pool different workloads/concurrency."""
import csv
import argparse
import json
from pathlib import Path
import statistics

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('input', type=Path, help='Directory containing measured/ and allocation/')
parser.add_argument('--out', type=Path, required=True, help='Directory for generated CSV files')
args = parser.parse_args()
ROOT = args.input.resolve()
OUT = args.out.resolve()
OUT.mkdir(parents=True, exist_ok=True)
rows = json.loads((ROOT/'measured/summary.json').read_text())
alloc = json.loads((ROOT/'allocation/summary.json').read_text())
environment = json.loads((ROOT/'measured/environment.json').read_text())
assert len(rows) == 2*4*2*4*2, len(rows)
assert len(alloc) == 2*2*2*4, len(alloc)
assert all(r['errors'] == 0 for r in rows+alloc)
assert len(set(environment['binary_sha256'].values())) == 2
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
