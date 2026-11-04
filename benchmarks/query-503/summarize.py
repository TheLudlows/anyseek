#!/usr/bin/env python3
"""Recompute fixed-work results from the retained response bodies."""
import argparse
import collections
import json
import math
from pathlib import Path


def quantiles(values):
    values = sorted(values)
    return {f'p{p}_ms': values[math.ceil(len(values) * p / 100) - 1] for p in (50, 95, 99)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('root', type=Path)
    parser.add_argument('--prefix', default='paired')
    args = parser.parse_args()
    assert __debug__, 'assertions required'
    results = []
    seeds, hashes = set(), collections.defaultdict(set)
    for rep in range(2):
        for version in ['baseline', 'candidate']:
            for endpoint, mode in [('batch', 'lexical'), ('batch', 'hybrid'), ('ingest', 'lexical')]:
                name = f'{args.prefix}-{rep}-{version}-{endpoint}-{mode}'
                folder = args.root / name
                summary = json.loads((folder / 'summary.json').read_text())
                rows = [json.loads(line) for line in (folder / 'requests.jsonl').read_text().splitlines()]
                assert len(rows) == 360
                identities = {(r['kind'], r['worker'], r['i']) for r in rows}
                assert identities == {('write', 0, i) for i in range(40)} | {('query', worker, i) for worker in range(4) for i in range(80)}
                assert (summary['endpoint'], summary['mode']) == (endpoint, mode)
                seeds.add(summary['seed_sha256'])
                hashes[version].add(summary['binary_sha256'])
                item = {'run': name}
                for kind in ['query', 'write']:
                    selected = [r for r in rows if r['kind'] == kind]
                    successful = [r for r in selected if r.get('status') == 200 and 'transport_error' not in r]
                    assert summary[kind]['success'] == len(successful)
                    assert summary[kind]['all'] == quantiles([r['ms'] for r in selected])
                    measured = quantiles([r['ms'] for r in successful]) if successful else None
                    assert summary[kind]['successful'] == measured
                    errors = collections.Counter(r.get('transport_error', f"{r.get('status')} {r.get('body')}") for r in selected if r not in successful)
                    assert dict(errors) == summary[kind]['errors']
                    item[kind] = {'success': len(successful), 'count': len(selected), 'errors': dict(errors), 'successful': measured}
                    if kind == 'write':
                        assert len(successful) == 40
                        for r in successful:
                            batch = json.loads(r['body'])['results']
                            assert len(batch) == 16
                            assert all(b['code'] == ('applied' if endpoint == 'batch' else 'committed') and b.get('receipt') for b in batch)
                assert summary['final_count'] == 640
                assert summary['progress'] == {'committed_seq': 1640, 'lexical_seq': 1640, 'vector_seq': 1640, 'index_error': None, 'recovery_required': False}
                item['queries_started_before_writer_end'] = summary['queries_started_before_writer_end']
                results.append(item)
    assert len(seeds) == 1 and all(len(v) == 1 for v in hashes.values())
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
