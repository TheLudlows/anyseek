"""Run a real release example (cargo test uses unwind even with --release)."""
import argparse
from pathlib import Path
import signal
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--probe', required=True)
    args = parser.parse_args()
    probe = str(Path(args.probe).resolve())
    for fault in ('vector', 'queue'):
        with tempfile.TemporaryDirectory(prefix='anyseek-release-panic-') as tmp:
            root = str(Path(tmp) / 'records')
            subprocess.run([probe, 'seed', root], check=True, timeout=60)
            crashed = subprocess.run([probe, fault, root], timeout=60)
            # Linux CI: require SIGABRT, not any nonzero exit or an assertion exit.
            assert crashed.returncode == -signal.SIGABRT, crashed.returncode
            subprocess.run([probe, 'verify-' + fault, root], check=True, timeout=60)
            print(f'PASS release panic={fault}: SIGABRT and authority replay', flush=True)


if __name__ == '__main__':
    main()
