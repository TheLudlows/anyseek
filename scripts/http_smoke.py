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

    retired = subprocess.run([str(binary), 'ingest_path=unused'], cwd=workdir,
                             capture_output=True, timeout=10)
    check(retired.returncode == 2 and b'ingest_path has been removed' in retired.stderr,
          'retired import configuration was silently accepted')
    print('PASS retired import configuration rejected')

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
            check('/js/autocomplete.js' not in assets.paths, 'retired autocomplete script referenced')
            check('id=suggestions' not in html, 'retired suggestions control present')
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
            for field in ('spelling_correction', 'query_completion'):
                request('POST', '/api/v1/index', {'index_name': 'retired', 'schema': schema,
                        field: {}}, api_key, expected=400)
            for field in ('dictionary_source', 'completion_source'):
                retired_schema = [{**item, field: True} for item in schema]
                request('POST', '/api/v1/index', {'index_name': 'retired', 'schema': retired_schema},
                        api_key, expected=400)
            for mode in ('SearchOnly', {'SearchRewrite': {'distance': 1}}):
                request('POST', path + '/query', {'query': 'test', 'query_rewriting': mode},
                        api_key, expected=400)
            request('GET', path + '/query?query=test&query_rewriting=SearchOnly',
                    key=api_key, expected=400)
            literal = api('POST', path + '/query', {'query': 'tset', 'search_mode': 'Lexical',
                          'result_type': 'TopkCount'})
            check(literal['query'] == literal['original_query'] == 'tset' and
                  literal['count_total'] == 0 and 'suggestions' not in literal,
                  f'query unexpectedly rewritten: {literal}')
            print('PASS retired query options rejected and literal query preserved')
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
            # The retired file endpoints must fail without adding a record.
            request('POST', path + '/file', b'%PDF-1.7 removed', api_key, expected=404)
            request('GET', path + '/file/0', key=api_key, expected=404)
            check(api('PATCH', path) == 4, 'retired PDF endpoint mutated index')
            check(not list((workdir / 'index').rglob('files')), 'created PDF attachment directory')
            # Keep update/delete/get covered when the core attachment parameter is removed.
            api('PATCH', path + '/doc', [doc_id, {'body': 'updated unique_record'}])
            check(api('PATCH', path) == 5, 'update did not finish')
            updated = api('POST', path + '/query', {
                'query': 'unique_record', 'search_mode': 'Lexical', 'result_type': 'TopkCount'})
            check(updated['count_total'] == 1, 'updated record not searchable')
            updated_id = updated['results'][0]['_id']
            check(api('GET', path + f'/doc/{updated_id}')['body'] == 'updated unique_record',
                  'updated record not readable')
            api('DELETE', path + f'/doc/{updated_id}')
            check(api('POST', path + '/query', {'query': 'unique_record', 'search_mode': 'Lexical',
                  'result_type': 'TopkCount'})['count_total'] == 0, 'deleted record still visible')
            print('PASS JSON create/write/commit/query/get/update/delete; PDF endpoints removed')

            vector_schema = [schema[1], {'field': 'category', 'field_type': 'String16', 'store': True,
                                        'index_lexical': False, 'facet': True}, {'field': 'vector', 'field_type': 'Json', 'store': True,
                                         'index_lexical': False, 'index_vector': True}]
            config = {'index_name': 'external_vectors', 'schema': vector_schema,
                      'inference': {'External': {'dimensions': 3, 'precision': 'F32',
                       'quantization': 'None', 'similarity': 'Cosine'}}, 'clustering': 'None'}
            vector_id = api('POST', '/api/v1/index', config)
            vp = f'/api/v1/index/{vector_id}'
            for invalid in ({'body': 'missing'}, {'body': 'bad', 'vector': [1, 2]},
                            {'body': 'zero', 'vector': [0, 0, 0]},
                            [{'body': 'partial', 'vector': [1, 0, 0]}, {'body': 'missing'}]):
                request('POST', vp + '/doc', invalid, api_key, expected=400)
            check(api('PATCH', vp) == 0, 'invalid batch partially indexed')
            api('POST', vp + '/doc', [{'body': 'alpha', 'vector': [1, 0, 0], 'category': 'a'},
                                     {'body': 'beta', 'vector': [0, 1, 0], 'category': 'b'}])
            check(api('PATCH', vp) == 2, 'external write count')
            for mode in ('Vector', 'Hybrid'):
                query_request = {'query': 'alpha', 'search_mode': {mode: {
                    'similarity_threshold': None, 'ann_mode': 'All'}}, 'result_type': 'TopkCount'}
                request('POST', vp + '/query', query_request, api_key, expected=400)
                for invalid in ([1], [0, 0, 0], ['x', 0, 0], '!base64',
                                base64.b64encode(bytes.fromhex('7f8000000000000000000000')).decode()):
                    request('POST', vp + '/query', {**query_request, 'query_vector': invalid},
                            api_key, expected=400)
                result = api('POST', vp + '/query', {**query_request, 'query_vector': [1, 0, 0]})
                check(result['results'][0]['body'] == 'alpha', f'{mode} ranking: {result}')
                # The inherited vector branch did not apply facet_filter. Fail closed until S2.
                rejected_filter = request('POST', vp + '/query', {**query_request, 'query_vector': [1, 0, 0],
                    'facet_filter': [{'String16': {'field': 'category', 'filter': ['b']}}]},
                    api_key, expected=400)
                check(b'vector candidate filtering is not implemented' in rejected_filter,
                      'vector facet_filter must fail explicitly after valid deserialization')
            filtered = api('POST', vp + '/query', {'query': '', 'enable_empty_query': True,
                'search_mode': 'Lexical', 'facet_filter': [{'String16': {'field': 'category', 'filter': ['b']}}]})
            check([doc['body'] for doc in filtered['results']] == ['beta'], 'lexical metadata filter')
            request('PATCH', vp + '/doc', [0, {'body': 'invalid update'}], api_key, expected=400)
            request('PATCH', vp + '/doc', [[0, {'body': 'changed', 'vector': [0, 0, 1]}],
                                           [1, {'body': 'invalid'}]], api_key, expected=400)
            lexical = api('POST', vp + '/query', {'query': 'alpha', 'search_mode': 'Lexical'})
            check(lexical['results'][0]['body'] == 'alpha', 'failed update lost original')
            request('POST', f'/api/v2/index/{vector_id}/query', b'invalid archive', api_key, expected=400)
            for inference in ({'Model2Vec': {'model': 'PotionBase2M', 'chunk_size': 1000}},
                              {'External': {'dimensions': 0, 'precision': 'F32', 'quantization': 'None', 'similarity': 'Cosine'}},
                              {'External': {'dimensions': 3, 'precision': 'F32', 'quantization': 'None', 'similarity': 'invalid'}}):
                request('POST', '/api/v1/index', {**config, 'inference': inference}, api_key, expected=400)
            binary_schema = [{**field, 'field_type': 'Binary'} if field['field'] == 'vector' else field
                             for field in vector_schema]
            binary_id = api('POST', '/api/v1/index', {**config, 'schema': binary_schema})
            bp = f'/api/v1/index/{binary_id}'
            encoded = base64.b64encode(bytes.fromhex('3f8000000000000000000000')).decode()
            api('POST', bp + '/doc', {'body': 'binary', 'vector': encoded})
            check(api('PATCH', bp) == 1, 'binary vector write')
            result = api('POST', bp + '/query', {'query': '', 'query_vector': encoded,
                'search_mode': {'Vector': {'similarity_threshold': None, 'ann_mode': 'All'}}})
            check(result['results'][0]['body'] == 'binary', 'base64 vector query')
            print('PASS external vector/hybrid, invalid input, batch and update atomic validation')
            if os.environ.get('SEEKPOWER_BROWSER_TEST') == '1':
                source_id = api('POST', '/api/v1/index', {
                    'index_name': 'source_links', 'schema': [
                        {**field, 'store': True} for field in schema]})
                sp = f'/api/v1/index/{source_id}'
                api('POST', sp + '/doc', [
                    {'title': 'External source', 'body': 'source document',
                     'url': 'https://source.example/report.pdf'},
                    {'title': 'Plain record', 'body': 'plaintext preview', 'url': ''}])
                api('PATCH', sp)
                from playwright.sync_api import sync_playwright
                with sync_playwright() as browser_driver:
                    browser = browser_driver.chromium.launch()
                    page = browser.new_page()
                    errors = []
                    page.on('pageerror', lambda error: errors.append(str(error)))
                    page.goto(base + f'/?index_id={vector_id}&q=alpha')
                    page.locator('#contentBox .resultEntry').first.wait_for()
                    check(page.locator('input[value="Lexical"]').is_checked(), 'UI default mode')
                    page.locator('label[data-index="0"]').click()
                    check('require' in page.locator('#searchError').inner_text(), 'UI missing-vector feedback')
                    page.locator('#queryVector').fill('[1,0,0]')
                    page.locator('#vectorSearch').click()
                    page.locator('#contentBox .resultEntry').first.wait_for()
                    page.locator('label[data-index="1"]').click()
                    page.locator('#contentBox .resultEntry').first.wait_for()
                    page.locator('#queryVector').fill('[1]')
                    with page.expect_response(lambda response: '/query' in response.url and response.status == 400):
                        page.locator('#vectorSearch').click()
                    page.wait_for_function("document.getElementById('searchError').textContent.length > 0")
                    # A PDF source is an ordinary external URL, never an internal /file call.
                    file_requests = []
                    page.on('request', lambda req: file_requests.append(req.url)
                            if '/api/v1/index/' in req.url and '/file' in req.url else None)
                    page.route('https://source.example/**', lambda route: route.fulfill(
                        content_type='text/html', body='<p>External source</p>'))
                    page.goto(base + f'/?index_id={source_id}&q=source')
                    source = page.locator('#contentBox a.url').first
                    source.wait_for()
                    check(source.get_attribute('href') == 'https://source.example/report.pdf',
                          'PDF URL rewritten to internal endpoint')
                    source.hover()
                    page.wait_for_function("document.getElementById('prevText').src === 'https://source.example/report.pdf'")
                    page.goto(base + f'/?index_id={source_id}&q=plaintext')
                    page.locator('#contentBox .resultEntry h1').first.hover()
                    page.wait_for_function("document.getElementById('prevText2').textContent.includes('plaintext')")
                    check(not file_requests, f'UI called removed file endpoints: {file_requests}')
                    search_input = page.locator('#searchMain')
                    with page.expect_response(lambda response: '/query' in response.url and
                            response.request.method == 'POST' and
                            response.request.post_data_json.get('query') == 'plaintxt') as typed:
                        search_input.fill('plaintxt')
                    typed_response = typed.value
                    check(typed_response.status == 200, 'plain typing search failed')
                    check('query_rewriting' not in typed_response.request.post_data_json,
                          'UI sent retired rewriting parameter')
                    check(typed_response.json()['count_total'] == 0, 'typo silently corrected')
                    check(search_input.input_value() == 'plaintxt', 'typed input was rewritten')
                    check(page.locator('#suggestions, #suggestionsMain, #did_you_mean').count() == 0,
                          'retired suggestions UI remains')
                    check(not errors, f'browser errors: {errors}')
                    browser.close()
                print('PASS Chromium lexical/vector/hybrid, source/text preview and unchanged typed query')

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
