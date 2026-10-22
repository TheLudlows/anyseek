#!/usr/bin/env python3
"""Build a standalone consumer so workspace feature unification cannot hide leaks."""
import json
from pathlib import Path
import subprocess
import tempfile
import re
import tomllib

core = Path(__file__).resolve().parents[1] / 'anyseek'
manifest = tomllib.loads((core / 'Cargo.toml').read_text())
assert not ({ 'num-format', 'tabled', 'chrono', 'colored', 'half', 'lz4_flex', 'zstd', 'snowball_stemmers_rs', 'gxhash'} & set(manifest['dependencies']))
assert not {'api-schema', 'gxhash'} & manifest.get('features', {}).keys()
for source in (core / 'src').rglob('*.rs'):
    text = source.read_text()
    assert not re.search(r'\b(?:e?println|e?print)!', text), f'direct core output: {source}'
    assert not re.search(r'\b(?:crate|anyseek)::server\b|\buse\s+.*\bserver\b', text), f'core depends on server: {source}'
    assert 'utoipa' not in text and 'api-schema' not in text, f'HTTP schema in core: {source}'
    assert not re.search(r'pub struct (?:SearchRequestObject|SearchResultObject|CreateIndexRequest|GetDocumentRequest|GetIteratorRequest|IndexResponseObject|RangeU8)\b', text), f'HTTP DTO in core: {source}'
for name in ('axum', 'http-body', 'env_logger', 'utoipa'):
    assert manifest['dependencies'][name].get('optional') is True, name
assert 'utoipa-axum' not in manifest['dependencies']
assert 'server' not in manifest['features']['default']
assert manifest['bin'][0]['required-features'] == ['server']
print('PASS core source/manifest have no HTTP DTO, schema or terminal output dependencies', flush=True)
with tempfile.TemporaryDirectory(prefix='anyseek-core-') as directory:
    consumer = Path(directory)
    (consumer / 'src').mkdir()
    (consumer / 'src/main.rs').write_text('fn main() { let _ = anyseek::index::TokenizerType::UnicodeAlphanumeric; }\n')
    (consumer / 'Cargo.toml').write_text(f'''[package]
name = "anyseek-core-check"
version = "0.0.0"
edition = "2024"
[workspace]
[features]
default = ["anyseek/default"]
[dependencies]
anyseek = {{ path = {json.dumps(str(core))}, default-features = false }}
''')
    for flags in ([], ['--no-default-features']):
        args = ['--manifest-path', str(consumer / 'Cargo.toml'), *flags]
        tree = subprocess.check_output(['cargo', 'tree', *args, '-e', 'normal,build', '--prefix', 'none'], text=True)
        names = {line.split()[0] for line in tree.splitlines() if line.strip()}
        forbidden = names & {'utoipa', 'utoipa-gen', 'env_logger', 'axum', 'http-body', 'utoipa-axum', 'num-format', 'tabled', 'chrono', 'colored', 'tokio-test',
                             'snowball_stemmers_rs', 'gxhash', 'anyseek_server', 'hyper', 'hyper-util', 'http-body-util', 'pdfium-render', 'csv', 'walkdir', 'symspell_complete_rs', 'model2vec-rs'}
        assert not forbidden, f'core runtime dependency leak: {forbidden}'
        subprocess.run(['cargo', 'check', *args], check=True)
        print(f'PASS isolated core dependency graph and build: {flags or "default features"}', flush=True)
