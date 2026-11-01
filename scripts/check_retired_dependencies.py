#!/usr/bin/env python3
"""Verify retired dependencies and the permitted native-only dependency edges."""
import json
from pathlib import Path
import subprocess

metadata = json.loads(subprocess.check_output(
    ['cargo', 'metadata', '--format-version', '1'],
    cwd=Path(__file__).resolve().parents[1], text=True,
))
packages = metadata['packages']
names = {package['name'] for package in packages}
forbidden = names & {'pdfium-render', 'csv', 'walkdir', 'symspell_complete_rs', 'model2vec-rs', 'rkyv', 'tabled', 'colored', 'num-format', 'ctrlc', 'half', 'snowball_stemmers_rs', 'gxhash'}
assert not forbidden, f'retired dependencies remain: {forbidden}'
core = next(package for package in packages if package['name'] == 'anyseek')
# Native Tantivy uses channels and LZ4 internally. Permit those only
# behind its dependency edge; do not restore old direct code paths.
native_only = {'crossbeam-channel', 'lz4_flex'}
assert not native_only & {d['name'] for d in core['dependencies']}
by_id = {p['id']: p for p in packages}
nodes = {n['id']: n for n in metadata['resolve']['nodes']}
seen, pending = set(), [core['id']]
while pending:
    current = pending.pop()
    if current in seen or by_id[current]['name'] == 'tantivy':
        continue
    seen.add(current)
    pending.extend(d['pkg'] for d in nodes[current]['deps'])
assert not native_only & {by_id[p]['name'] for p in seen}, 'native-only dependency escaped Tantivy'
# Jieba's embedded dictionary uses include-flate/Zstd. Permit it only
# beneath jieba-rs, never as a direct/alternative storage codec.
dictionary_only = {'zstd', 'zstd-safe', 'zstd-sys'}
seen, pending = set(), [core['id']]
while pending:
    current = pending.pop()
    if current in seen or by_id[current]['name'] == 'jieba-rs':
        continue
    seen.add(current)
    pending.extend(d['pkg'] for d in nodes[current]['deps'])
assert not dictionary_only & {by_id[p]['name'] for p in seen}, 'dictionary codec escaped Jieba'
assert not any(dependency['name'] == 'aho-corasick' for dependency in core['dependencies']), 'direct highlighter dependency remains'
# The tokenizer retains regex-syntax; HTTP dependencies are part of the service.
print('PASS no direct highlighter dependency')
print('PASS no PDF/CSV/directory parser or query rewriting dependencies in resolved graph')
