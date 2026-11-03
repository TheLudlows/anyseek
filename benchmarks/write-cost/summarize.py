#!/usr/bin/env python3
"""Recompute per-process and paired summaries from retained raw HTTP evidence."""
import argparse
import csv
import gzip
import json
import math
from pathlib import Path
import statistics


def percent(values, p):
    values = sorted(values)
    return values[math.ceil(len(values)*p/100)-1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    rows, strata, aggregated = [], [], {}
    for path in sorted(args.root.glob('*/summary.json')):
        summary = json.loads(path.read_text())
        assert summary['status'] == 'passed', path
        with gzip.open(path.parent/'requests.jsonl.gz', 'rt') as file:
            raw = [json.loads(line) for line in file]
        for group in summary['groups']:
            samples = [r for r in raw if r['kind'] == group['kind'] and not r['warmup']]
            assert len(samples) == group['calls']
            assert sum(r['busy'] for r in samples) == group['busy']
            for metric in ('ack_ms', 'catchup_ms', 'complete_ms'):
                for p in (50,95,99):
                    assert percent([r[metric] for r in samples], p) == group[metric][f'p{p}_ms']
                assert max(r[metric] for r in samples) == group[metric]['max_ms']
            row = {k: summary[k] for k in ('run','version','n','endpoint','batch','rep','allocation_run')}
            row.update(kind=group['kind'], calls=group['calls'], records=group['records'], busy=group['busy'], busy_items=group.get('busy_items',0),
                       records_s=group['wall_records_s'], cycle_records_s=group['completed_records_s'], cpu_ms_per_record=group['server_cpu_s']*1000/group['records'],
                       hwm_mib=group['lifetime_hwm_kib']/1024,
                       rss_after_mib=group['rss_after_kib']/1024, cache_creation_calls=group['cache_creation_calls'])
            for metric in ('ack','catchup','complete'):
                for stat in ('p50','p95','p99','max'):
                    row[f'{metric}_{stat}_ms'] = group[metric+'_ms'][stat+'_ms']
            if group['allocation_calls_bytes']:
                row['allocation_calls_per_record'], row['allocation_bytes_per_record'] = [x/group['records'] for x in group['allocation_calls_bytes']]
            rows.append(row)
            key = (summary['n'],summary['endpoint'],summary['batch'],group['kind'])
            aggregated.setdefault(key, {}).setdefault(summary['version'], []).append(row)
            for boundary in (False, True):
                selected = [r for r in samples if bool(r['new_cache_dirs']) == boundary]
                if selected:
                    strata.append({'run': summary['run'], 'kind': group['kind'], 'cache_created': boundary,
                                   'calls': len(selected), 'ack_p95_ms': percent([r['ack_ms'] for r in selected],95),
                                   'ack_max_ms': max(r['ack_ms'] for r in selected),
                                   'complete_p95_ms': percent([r['complete_ms'] for r in selected],95)})
    comparisons = []
    for key, versions in sorted(aggregated.items()):
        if 'before' not in versions:
            continue
        for other in ('after', 'latest'):
            if other not in versions:
                continue
            row = dict(zip(('n','endpoint','batch','kind'),key), compared_to_before=other)
            for metric in ('records_s','cpu_ms_per_record','ack_p95_ms','ack_p99_ms','ack_max_ms','complete_p95_ms','rss_after_mib','allocation_calls_per_record','allocation_bytes_per_record'):
                if metric not in versions['before'][0]:
                    continue
                a,b = [statistics.median(r[metric] for r in versions[v]) for v in ('before',other)]
                row[metric+'_before'] = a
                row[metric+'_other'] = b
                row[metric+'_pct'] = 100*(b/a-1) if a else None
            comparisons.append(row)
    for name, values in [('processes',rows),('cache-strata',strata),('comparison',comparisons)]:
        if values:
            with (args.root/(name+'.csv')).open('w',newline='') as file:
                writer=csv.DictWriter(file, fieldnames=list(values[0]), lineterminator="\n");writer.writeheader();writer.writerows(values)
    print(f'verified {len(rows)} process/scenario windows, {sum(r["calls"] for r in rows)} measured calls')


if __name__ == '__main__':
    main()
