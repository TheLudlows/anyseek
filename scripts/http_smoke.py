#!/usr/bin/env python3
"""Exercise the retained HTTP/UI surface without a published client SDK.

Requires only Python 3 and an already-built seekstorm_server. All writes go to a
new temporary index directory; the child is stopped even if an assertion fails.
Migrates the former SDK live/key/index/write/commit/query/get/clear scenarios.
"""
import argparse
import base64
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener


class Assets(HTMLParser):
    def __init__(self):
        super().__init__()
        self.paths = set()

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        value = attrs.get('src') if tag in ('script', 'img') else None
        if tag == 'link':
            value = attrs.get('href')
        if value and not urlsplit(value).scheme and not value.startswith('//'):
            self.paths.add('/' + value.lstrip('/'))


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def exercise(binary, workdir):
    # Reserve an OS-selected high port until immediately before launching.
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    base = f'http://127.0.0.1:{port}'
    opener = build_opener(ProxyHandler({}))
    secret = secrets.token_hex(24)
    master = base64.b64encode(hashlib.sha256(secret.encode()).digest()).decode()
    api_key = None

    def request(method, path, data=None, key=None, expected=200):
        headers = {}
        if key is not None:
            headers['apikey'] = key
        if data is not None and not isinstance(data, bytes):
            data = json.dumps(data, ensure_ascii=False).encode()
            headers['Content-Type'] = 'application/json'
        req = Request(base + path, data=data, headers=headers, method=method)
        try:
            response = opener.open(req, timeout=10)
        except HTTPError as error:
            response = error
        with response:
            body = response.read()
            check(response.status == expected,
                  f'{method} {path}: expected {expected}, got {response.status}: {body!r}')
            return body

    def api(method, path, data=None):
        value = json.loads(request(method, path, data, api_key))
        if isinstance(value, dict):
            check('Err' not in value, f'{method} {path}: {value}')
            if set(value) == {'Ok'}:
                return value['Ok']
        return value

    log_path = workdir / 'server.log'
    with log_path.open('wb') as log:
        process = subprocess.Popen(
            [str(binary), 'local_ip=127.0.0.1', f'local_port={port}',
             f'index_path={workdir / "index"}', 'force_shard_number=1'],
            cwd=workdir, env={**os.environ, 'MASTER_KEY_SECRET': secret},
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 30
            while True:
                check(process.poll() is None, 'server exited before becoming ready')
                try:
                    request('GET', '/api/v1/live')
                    break
                except (URLError, TimeoutError, ConnectionError):
                    if time.monotonic() >= deadline:
                        raise RuntimeError('server did not become ready within 30 seconds')
                    time.sleep(0.1)
            print('PASS health')

            html = request('GET', '/').decode()
            check('searchInput' in html and 'searchMain' in html, 'search UI missing')
            assets = Assets()
            assets.feed(html)
            check('/js/master.js' in assets.paths and '/css/master.css' in assets.paths,
                  'UI asset references missing')
            for path in sorted(assets.paths):
                body = request('GET', path)
                check(body and not body.lstrip().lower().startswith(b'<!doctype html'),
                      f'asset missing or served HTML: {path}')
            print(f'PASS UI HTML and {len(assets.paths)} local assets')

            request('GET', '/api/v1/index/0', expected=401)
            request('POST', '/api/v1/apikey', {}, key='invalid', expected=401)
            quota = {'indices_max': 10, 'indices_size_max': 100000000,
                     'documents_max': 10000, 'operations_max': 100000,
                     'rate_limit': None, 'demo': True}
            api_key = request('POST', '/api/v1/apikey', quota, master).decode()
            check(api_key == base64.b64encode(bytes(32)).decode(), 'demo key mismatch')
            print('PASS authentication and key creation')

            schema = [
                {'field': 'title', 'field_type': 'Text', 'store': False, 'index_lexical': False},
                {'field': 'body', 'field_type': 'Text', 'store': True, 'index_lexical': True,
                 'longest': True},
                {'field': 'url', 'field_type': 'Text', 'store': False, 'index_lexical': False}]
            index_id = api('POST', '/api/v1/index', {
                'index_name': 'http_smoke', 'schema': schema, 'similarity': 'Bm25f',
                'tokenizer': 'UnicodeAlphanumeric', 'stemmer': 'None', 'stop_words': 'None',
                'frequent_words': 'English', 'ngram_indexing': 5,
                'document_compression': 'Snappy', 'inference': 'None', 'clustering': 'None'})
            check(index_id == 0, f'first index ID mismatch: {index_id}')
            path = f'/api/v1/index/{index_id}'
            api('POST', path + '/doc', {'title': 'title1 test', 'body': 'body1', 'url': 'url1'})
            api('POST', path + '/doc', [
                {'title': 'title1 test', 'body': 'body1', 'url': 'url1'},
                {'title': 'title2', 'body': 'body2 test', 'url': 'url2'},
                {'title': 'title3 test', 'body': 'body3 test', 'url': 'url3'}])
            check(api('PATCH', path) == 4, 'commit must expose four documents')
            result = api('POST', path + '/query', {
                'query': '+body2 +test', 'result_type': 'TopkCount',
                'search_mode': 'Lexical', 'realtime': False})
            check(result['count_total'] == 1 and len(result['results']) == 1,
                  f'lexical query mismatch: {result}')
            doc_id = result['results'][0]['_id']
            document = api('GET', path + f'/doc/{doc_id}', {
                'query_terms': [], 'highlights': [{'field': 'body', 'fragment_number': 2,
                                                 'fragment_size': 160, 'highlight_markup': True}]})
            check(document == {'body': 'body2 test'}, f'stored document mismatch: {document}')
            print('PASS create, single/batch write, commit, lexical query and get')

            check(api('DELETE', path + '/doc', b'clear') == 0, 'clear count mismatch')
            check(api('GET', path)['indexed_doc_count'] == 0, 'index not empty after clear')
            print('PASS clear and index info')
        except BaseException:
            log.flush()
            print(log_path.read_text(errors='replace'))
            raise
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, required=True)
    args = parser.parse_args()
    binary = args.server.resolve()
    if not binary.is_file():
        parser.error(f'server binary not found: {binary}; run cargo build -p seekstorm_server')
    with tempfile.TemporaryDirectory(prefix='seekpower-http-') as directory:
        exercise(binary, Path(directory))
    print('PASS HTTP/UI smoke')


if __name__ == '__main__':
    main()
