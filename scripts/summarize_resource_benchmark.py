#!/usr/bin/env python3
"""Recompute before/after resource experiments from retained request logs."""
import argparse, collections, json, math, pathlib, statistics

def summarize(root, tag):
    directory = root / 'raw' / tag
    result = json.loads((directory / 'run.json').read_text())
    groups = []
    for concurrency in (1, 4, 8):
        runs = [r for r in result.get('warm', []) if r['concurrency'] == concurrency]
        if not runs:
            continue
        times, statuses = [], collections.Counter()
        for run in runs:
            for line in (directory / (run['label'] + '.jsonl')).open():
                row = json.loads(line)
                statuses[str(row['status'])] += 1
                if row['status'] == 200:
                    times.append(row['ms'])
        times.sort()
        groups.append({'concurrency': concurrency, 'qps_median': statistics.median(r['qps'] for r in runs),
            'successful_latency_ms': {str(p): times[max(0, math.ceil(len(times)*p/100)-1)] for p in (50,95,99)} if times else {},
            'status_counts': dict(statuses), 'error_rate': 1-len(times)/max(1,sum(statuses.values()))})
    if 'mixed_query' in result:
        mixed = collections.Counter()
        for line in (directory / 'during-writes.jsonl').open():
            row = json.loads(line)
            mixed[(row['status'], str(row['error']))] += 1
        result['mixed_status_counts'] = [{'status': k[0], 'error': k[1], 'count': n} for k,n in mixed.items()]
    return {**result, 'aggregated': groups}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', type=pathlib.Path, required=True)
    parser.add_argument('--adjusted', type=pathlib.Path, required=True)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    args = parser.parse_args()
    old = ['anyseek-default-1000', 'anyseek-default-write1000', 'anyseek-restricted-1000', 'anyseek-default-5000']
    new = ['adjusted-default-1000','adjusted-restricted-1000','adjusted-default-5000','adjusted-restricted-5000','adjusted-default-1000-write']
    result = {'baseline': {tag:summarize(args.baseline, tag) for tag in old},
              'adjusted': {tag:summarize(args.adjusted, tag) for tag in new}}
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
