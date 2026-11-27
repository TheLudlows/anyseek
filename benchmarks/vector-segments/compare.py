"""Manual Linux paired process measurements; no performance pass threshold."""
import argparse
from collections import Counter, defaultdict
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--before', type=Path, required=True)
parser.add_argument('--after', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--cpus', help='optional Linux affinity, e.g. 1,2')
parser.add_argument('--pairs', type=int, default=5)
parser.add_argument('--matrix', action='store_true', help='profile filter selectivity, reopen and concurrency')
parser.add_argument('--trace', action='store_true', help='matrix only: separate untimed strace pass')
args = parser.parse_args()
if args.pairs < 1 or (args.trace and not args.matrix):
    parser.error('use one or more pairs and enable --matrix for --trace')
if args.cpus:
    os.sched_setaffinity(0, {int(cpu) for cpu in args.cpus.split(',')})
args.output.mkdir(parents=True, exist_ok=True)
results = []
environment = dict(os.environ)
if args.matrix:
    environment['ANYSEEK_SEGMENT_PROFILE_MATRIX'] = '1'

def command(version):
    return [str(getattr(args, version).resolve()), 'segment_query_profile',
            '--exact', '--ignored', '--test-threads=1']

for run in range(1, args.pairs + 1):
    for version in (['before', 'after'] if run % 2 else ['after', 'before']):
        output = (args.output / f'{version}-{run}.json').resolve()
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        start = time.monotonic()
        with (args.output / f'{version}-{run}.log').open('w') as log:
            subprocess.run(command(version), check=True,
                           env={**environment, 'ANYSEEK_SEGMENT_PROFILE_OUTPUT': str(output)},
                           stdout=log, stderr=subprocess.STDOUT)
        elapsed = time.monotonic() - start
        end = resource.getrusage(resource.RUSAGE_CHILDREN)
        results.append({'version': version, 'run': run, 'process_wall_s': elapsed,
                        'process_cpu_s': end.ru_utime + end.ru_stime - usage.ru_utime - usage.ru_stime,
                        'profile': json.loads(output.read_text())})
expected = [row['hits'] for row in results[0]['profile']['rows']]
assert all([row['hits'] for row in result['profile']['rows']] == expected for result in results)
(args.output / 'results.json').write_text(json.dumps({'runs': results}, indent=2) + '\n')
print(f'PASS exact hit/score/channel bits across all {len(results)} processes')

def trace_summary(prefix):
    events = []
    mappings = Counter()
    read = re.compile(r'pread64\(\d+<([^>]+/(metadata|vectors)\.bin)>, .*?, (\d+), (\d+)\) += (-?\d+)')
    stderr = re.compile(r'write\(2(?:<[^>]*>)?, "((?:[^"\\]|\\.)*)", \d+\)')
    marker = re.compile(r'ANYSEEK_PROFILE (BEGIN|END) (\S+) (\d+)\\n')
    mapping = re.compile(r'mmap\(.*<[^>]+/(metadata|vectors)\.bin>')
    for path in prefix.parent.glob(prefix.name + '.*'):
        pending = ''
        with path.open() as stream:
            for line in stream:
                time, _, call = line.partition(' ')
                if match := stderr.match(call):
                    chunk = match[1]
                    if chunk.startswith(('ANYSEEK_PROFILE BEGIN ', 'ANYSEEK_PROFILE END ')):
                        pending, marker_time = '', float(time)
                    elif not pending:
                        continue
                    pending += chunk
                    if '\\n' in pending:
                        if boundary := marker.match(pending):
                            events.append((marker_time, 'marker', boundary.groups()))
                        pending = ''
                elif match := read.match(call):
                    events.append((float(time), 'read', match.groups()))
                elif match := mapping.match(call):
                    mappings[match[1]] += 1
    rows = {}
    active = None
    for _, kind, fields in sorted(events):
        if kind == 'marker':
            boundary, label, round_id = fields
            if boundary == 'BEGIN':
                assert active is None
                active = f'{label}-{"reopen" if round_id == "0" else "warm"}'
                rows[active] = defaultdict(list)
            else:
                assert active is not None
                active = None
        elif active is not None:
            path, component, length, offset, returned = fields
            rows[active][component].append((path, int(offset), int(length), int(returned)))
    assert active is None and len(rows) == 24, f'incomplete trace: {prefix}'
    summary = {}
    for key, components in rows.items():
        summary[key] = {}
        for name in ('metadata', 'vectors'):
            reads = components.get(name, [])
            unique = set()
            previous_end = {}
            noncontiguous = 0
            for path, offset, length, returned in reads:
                unique.add((path, offset, length))
                noncontiguous += path in previous_end and offset != previous_end[path]
                previous_end[path] = offset + returned
            summary[key][name] = {
                'calls': len(reads), 'requested_bytes': sum(row[2] for row in reads),
                'returned_bytes': sum(max(0, row[3]) for row in reads),
                'repeated_ranges': len(reads) - len(unique),
                'noncontiguous_ranges': noncontiguous,
            }
    return {'all_process_file_mmaps': mappings, 'query_rounds': summary}

if args.trace:
    for version in ('before', 'after'):
        output = (args.output / f'{version}-trace-profile.json').resolve()
        prefix = args.output / f'{version}-syscalls'
        with (args.output / f'{version}-trace.log').open('w') as log:
            subprocess.run([
                'strace', '-ff', '--seccomp-bpf', '-qq', '-ttt', '-T', '-yy', '-s', '256',
                '-o', str(prefix), '-e', 'trace=write,pread64,mmap', *command(version), '--nocapture',
            ], check=True, env={
                **environment, 'ANYSEEK_SEGMENT_PROFILE_TRACE': '1',
                'ANYSEEK_SEGMENT_PROFILE_OUTPUT': str(output),
            }, stdout=log, stderr=subprocess.STDOUT)
        summary = trace_summary(prefix)
        (args.output / f'{version}-trace.json').write_text(json.dumps(summary, indent=2) + '\n')
