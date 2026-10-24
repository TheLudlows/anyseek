#!/usr/bin/env python3
"""Guard the Windows split against lost cases and successful zero-test runs."""
import argparse
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
PREFIXES = (
    'five_term_or_subsets_keep_all_pair_contributions_',
    'mixed_rle_array_bitmap_membership_scores_and_order_',
)
LARGE = {prefix + policy for prefix in PREFIXES for policy in ('shared', 'per_field')}
SMALL = {
    'append_changes_representative_but_not_upper_bound_safety',
    'partial_heap_cannot_mask_later_two_term_winner',
}
COMMAND = ['cargo', 'test', '-p', 'anyseek', '--test', 'lexical_correctness',
           '--features', 'server,fault-injection']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', choices=sorted(LARGE))
    args = parser.parse_args()
    workflow = (ROOT / '.github/workflows/ci.yml').read_text()
    section = workflow.split('  windows-lexical:\n', 1)[1].split('  s3-native-acceptance:', 1)[0]
    matrix = re.findall(r'^          - (\w+)$', section, re.MULTILINE)
    assert len(matrix) == len(LARGE) and set(matrix) == LARGE, matrix
    remainder = workflow.split('      - name: Native retained public Record integration contracts (Windows remainder)', 1)[1].split('      - name:', 1)[0]
    skips = re.findall(r'--skip (\S+)', remainder)
    assert set(skips) == set(PREFIXES), skips
    result = subprocess.run(COMMAND + ['--', '--list'], cwd=ROOT, check=True,
                            stdout=subprocess.PIPE, text=True)
    inventory = set(re.findall(r'^(\w+): test$', result.stdout, re.MULTILINE))
    assert inventory == LARGE | SMALL, inventory
    excluded = {name for name in inventory if any(skip in name for skip in skips)}
    assert excluded == LARGE and inventory - excluded == SMALL
    print('PASS partition: four full-size policy cases + two dual-policy cases; no lost or duplicated cases', flush=True)
    if args.case:
        subprocess.run(COMMAND + [args.case, '--', '--exact', '--nocapture'], cwd=ROOT, check=True)


if __name__ == '__main__':
    main()
