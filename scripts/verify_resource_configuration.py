#!/usr/bin/env python3
"""Check invalid budget overrides are rejected at service startup, before listening."""
import argparse, json, os, pathlib, subprocess, tempfile
parser = argparse.ArgumentParser()
parser.add_argument('--server', type=pathlib.Path, required=True)
parser.add_argument('--report', type=pathlib.Path, required=True)
args = parser.parse_args()
assert not args.report.exists(), 'refuse to overwrite evidence'
cases = [
    ({'ANYSEEK_BUILD_MEMORY_BYTES': 'not-a-number'}, 'invalid ANYSEEK_BUILD_MEMORY_BYTES'),
    ({'ANYSEEK_BUILD_MEMORY_BYTES': str(64*1024*1024-1)}, 'invalid build memory hierarchy'),
    ({'ANYSEEK_BUILD_MEMORY_BYTES': str(5*1024**3)}, 'invalid build memory hierarchy'),
    ({'ANYSEEK_BUILD_SHARED_MEMORY_BYTES': str(8*1024**3+1)}, 'invalid build memory hierarchy'),
    ({'ANYSEEK_FOREGROUND_PER_COLLECTION': '0'}, 'invalid ANYSEEK_FOREGROUND_PER_COLLECTION'),
    ({'ANYSEEK_FOREGROUND_PER_COLLECTION': '33'}, 'invalid FOREGROUND resource hierarchy'),
    ({'ANYSEEK_BACKGROUND_BYTES': str(8*1024**3+1)}, 'invalid BACKGROUND resource hierarchy'),
    ({'ANYSEEK_FOREGROUND_SLOTS': '48'}, 'resource limits must preserve catalog control headroom'),
]
results = []
for config, expected in cases:
    with tempfile.TemporaryDirectory(prefix='anyseek-budget-config-') as root:
        env = {k: v for k, v in os.environ.items() if not k.startswith('ANYSEEK_')}
        env.update(config)
        env['MASTER_KEY_SECRET'] = 'resource-config-fixture'
        result = subprocess.run([str(args.server.resolve()), 'local_ip=127.0.0.1',
                                 'local_port=0', 'index_path='+str(pathlib.Path(root)/'data')], env=env,
                                capture_output=True, text=True, timeout=15)
        output = '\n'.join(line for line in (result.stdout + result.stderr).splitlines()
                           if not line.startswith('Master API key:'))
        assert result.returncode != 0, (config, output)
        assert expected in output, (config, result.returncode, output)
        results.append({'config': config, 'exit_code': result.returncode,
                        'expected_diagnostic': expected, 'output': output})
args.report.parent.mkdir(parents=True, exist_ok=True)
args.report.write_text(json.dumps(results, ensure_ascii=False, indent=2)+'\n')
print(f'PASS {len(results)} invalid configurations rejected before serving')
