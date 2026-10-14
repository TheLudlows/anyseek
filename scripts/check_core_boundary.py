#!/usr/bin/env python3
"""Build a standalone consumer so workspace feature unification cannot hide leaks."""
import json
from pathlib import Path
import subprocess
import tempfile
import re
import tomllib

core = Path(__file__).resolve().parents[1] / 'seekstorm'
manifest = tomllib.loads((core / 'Cargo.toml').read_text())
assert not ({'utoipa', 'num-format', 'tabled', 'chrono', 'colored', 'env_logger'} & set(manifest['dependencies']))
assert 'api-schema' not in manifest.get('features', {})
for source in (core / 'src').glob('*.rs'):
    text = source.read_text()
    assert not re.search(r'\b(?:e?println|e?print)!', text), f'direct core output: {source}'
    assert 'utoipa' not in text and 'api-schema' not in text, f'HTTP schema in core: {source}'
    assert not re.search(r'pub struct (?:SearchRequestObject|SearchResultObject|CreateIndexRequest|GetDocumentRequest|GetIteratorRequest|IndexResponseObject|RangeU8)\b', text), f'HTTP DTO in core: {source}'
print('PASS core source/manifest have no HTTP DTO, schema or terminal output dependencies', flush=True)
with tempfile.TemporaryDirectory(prefix='seekpower-core-') as directory:
    consumer = Path(directory)
    (consumer / 'src').mkdir()
    (consumer / 'src/main.rs').write_text('fn main() { let _ = seekstorm::index::TokenizerType::UnicodeAlphanumeric; }\n')
    (consumer / 'Cargo.toml').write_text(f'''[package]
name = "seekpower-core-check"
version = "0.0.0"
edition = "2024"
[workspace]
[features]
default = ["seekstorm/default"]
[dependencies]
seekstorm = {{ path = {json.dumps(str(core))}, default-features = false }}
''')
    for flags in ([], ['--no-default-features']):
        args = ['--manifest-path', str(consumer / 'Cargo.toml'), *flags]
        tree = subprocess.check_output(['cargo', 'tree', *args, '-e', 'normal,build', '--prefix', 'none'], text=True)
        names = {line.split()[0] for line in tree.splitlines() if line.strip()}
        forbidden = names & {'utoipa', 'utoipa-gen', 'num-format', 'env_logger', 'tabled', 'chrono', 'colored', 'tokio-test',
                             'seekstorm_server', 'hyper', 'hyper-util', 'http-body-util', 'pdfium-render', 'csv', 'walkdir', 'symspell_complete_rs', 'model2vec-rs'}
        assert not forbidden, f'core runtime dependency leak: {forbidden}'
        subprocess.run(['cargo', 'check', *args], check=True)
        print(f'PASS isolated core dependency graph and build: {flags or "default features"}', flush=True)
