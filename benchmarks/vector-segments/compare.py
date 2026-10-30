"""Manual Linux paired process measurements; no performance pass threshold."""
import argparse
import json
import os
from pathlib import Path
import resource
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--before', type=Path, required=True)
parser.add_argument('--after', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--cpus', help='optional Linux affinity, e.g. 1,2')
args = parser.parse_args()
if args.cpus:
    os.sched_setaffinity(0, {int(cpu) for cpu in args.cpus.split(',')})
args.output.mkdir(parents=True, exist_ok=True)
results = []
for run in range(1, 6):
    for version in (['before', 'after'] if run % 2 else ['after', 'before']):
        output = (args.output / f'{version}-{run}.json').resolve()
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        start = time.monotonic()
        with (args.output / f'{version}-{run}.log').open('w') as log:
            subprocess.run([str(getattr(args, version).resolve()), 'segment_query_profile',
                            '--exact', '--ignored', '--test-threads=1'], check=True,
                           env={**os.environ, 'ANYSEEK_SEGMENT_PROFILE_OUTPUT': str(output)},
                           stdout=log, stderr=subprocess.STDOUT)
        elapsed = time.monotonic() - start
        end = resource.getrusage(resource.RUSAGE_CHILDREN)
        results.append({'version': version, 'run': run, 'process_wall_s': elapsed,
                        'process_cpu_s': end.ru_utime + end.ru_stime - usage.ru_utime - usage.ru_stime,
                        'profile': json.loads(output.read_text())})
expected = [row['hits'] for row in results[0]['profile']['rows']]
assert all([row['hits'] for row in result['profile']['rows']] == expected for result in results)
(args.output / 'results.json').write_text(json.dumps({'runs': results}, indent=2) + '\n')
print('PASS exact hit/score/channel bits across all ten processes')
