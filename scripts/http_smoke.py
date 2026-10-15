#!/usr/bin/env python3
"""Exercise the retained HTTP/UI surface without a published client SDK.

Requires only Python 3 and an already-built seekpower_server. All writes go to a
new temporary index directory; the child is stopped even if an assertion fails.
Migrates the former SDK live/key/index/write/commit/query/get/clear scenarios.
"""
import argparse
import base64
import hashlib
from html.parser import HTMLParser
import json
import os
import re
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlencode
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

    def request(method, path, data=None, key=None, expected=200, expected_headers=None):
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
            if expected >= 400 and method != 'HEAD':
                check(response.headers.get_content_type() == 'application/json', 'non-JSON error')
                error = json.loads(body)
                check(set(error) == {'code', 'message'} and isinstance(error['message'], str), 'error shape')
                check(error['code'] == {400:'bad_request',401:'unauthorized',404:'not_found',405:'method_not_allowed',500:'internal_error'}[expected], 'error category')
            for name, value in (expected_headers or {}).items():
                check(response.headers.get(name) == value,
                      f'{method} {path}: header {name}: {response.headers.get(name)!r} != {value!r}')
            return body

    def api(method, path, data=None):
        return json.loads(request(method, path, data, api_key))

    retired = subprocess.run([str(binary), 'ingest_path=unused'], cwd=workdir,
                             capture_output=True, timeout=10)
    check(retired.returncode == 2 and b'ingest_path has been removed' in retired.stderr,
          'retired import configuration was silently accepted')
    print('PASS retired import configuration rejected')
    for args in (['unknown=value'], ['local_port=bad'], ['force_shard_number=0'],
                 ['generate_openapi=yes'], ['index_path='], ['bare_argument'],
                 ['local_port=80', 'local_port=81']):
        rejected = subprocess.run([str(binary), *args], cwd=workdir, capture_output=True, timeout=10)
        check(rejected.returncode == 2, f'unsupported startup arguments accepted: {args}')
    print('PASS invalid/unknown/duplicate startup options rejected')


    log_path = workdir / 'server.log'
    with log_path.open('wb') as log:
        def launch():
            return subprocess.Popen(
                [str(binary), 'local_ip=127.0.0.1', f'local_port={port}',
                 f'index_path={workdir / "index"}', 'force_shard_number=1'],
                cwd=workdir, env={**os.environ, 'MASTER_KEY_SECRET': secret},
                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)

        def ready(process):
            deadline = time.monotonic() + 30
            while True:
                check(process.poll() is None, 'server exited before becoming ready')
                try:
                    request('GET', '/api/v1/live')
                    return
                except (URLError, TimeoutError, ConnectionError):
                    if time.monotonic() >= deadline:
                        raise RuntimeError('server did not become ready within 30 seconds')
                    time.sleep(0.1)

        process = launch()
        try:
            ready(process)
            print('PASS health')
            check(re.findall(r'^Master API key: (.+)$', log_path.read_text(), re.MULTILINE) == [master],
                  'startup did not expose the configured master key')
            check(json.loads(request('GET', '/api/v1/apikey', key=master)) == [],
                  'master key must be directly usable with an empty scope')
            master_identity = (workdir / 'index' / '0' / 'apikey.json').read_bytes()
            check(master.encode() not in master_identity, 'plaintext key persisted in identity')
            master_id = json.loads(request('POST', '/api/v1/index', {
                'index_name': 'master_scope', 'schema': [
                    {'field': 'body', 'field_type': 'Text', 'store': True, 'index_lexical': True}]}, master))
            master_path = f'/api/v1/index/{master_id}'
            request('POST', master_path + '/doc', {'body': 'master_record'}, master)
            request('PATCH', master_path, key=master)
            process.terminate()
            process.wait(timeout=10)
            process = launch()
            ready(process)
            check(json.loads(request('POST', master_path + '/query', {'query': 'master_record'}, master))['count_total'] == 1,
                  'master-owned documents did not survive restart')
            check((workdir / 'index' / '0' / 'apikey.json').read_bytes() == master_identity,
                  'restart changed master identity')
            request('DELETE', master_path, key=master)
            check(len(list((workdir / 'index').rglob('apikey.json'))) == 1,
                  'startup created an unsolicited ordinary key')
            check(not (workdir / 'index' / 'bootstrap.complete').exists(), 'obsolete bootstrap marker created')
            print('PASS direct master-key create/write/query and restart persistence')

            html = request('GET', '/').decode()
            check(master not in html, 'credentials leaked into public HTML')
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
            check(not any('histogram' in path or 'bootstrap-slider' in path for path in assets.paths), 'retired chart assets referenced')
            print(f'PASS UI HTML and {len(assets.paths)} local assets')

            request('GET', '/api/v1/index/0', expected=401)
            request('POST', '/api/v1/apikey', {}, key='invalid', expected=401)
            api_key = json.loads(request('POST', '/api/v1/apikey', {}, master))
            for field in ('indices_max', 'indices_size_max', 'documents_max', 'operations_max', 'rate_limit', 'quota', 'demo'):
                request('POST', '/api/v1/apikey', {field: 0}, master, expected=400)
                request('POST', '/api/v1/apikey?' + urlencode({field: 0}), {}, master, expected=400)
            request('POST', '/api/v1/apikey', {'demo': True}, api_key, expected=401)
            check(len(base64.b64decode(api_key)) == 32 and api_key not in (master, base64.b64encode(bytes(32)).decode()), 'random key invalid')
            request('GET', '/api/v1/apikey', key=base64.b64encode(bytes(32)).decode(), expected=401)
            request('GET', '/version', expected=404)
            for invalid in (b'[]', b'null', b'false', b'""', b'{"demo":false}', b'{"demo":true}'):
                request('POST', '/api/v1/apikey', invalid, master, expected=400)
            print('PASS authentication and key creation')

            schema = [
                {'field': 'title', 'field_type': 'Text', 'store': False, 'index_lexical': False},
                {'field': 'body', 'field_type': 'Text', 'store': True, 'index_lexical': True},
                {'field': 'url', 'field_type': 'Text', 'store': False, 'index_lexical': False}]
            index_id = api('POST', '/api/v1/index', {'index_name': 'http_smoke', 'schema': schema, 'tokenizer': 'UnicodeAlphanumeric', 'stop_words': 'None'})
            check(index_id == 0, f'first index ID mismatch: {index_id}')
            path = f'/api/v1/index/{index_id}'
            api('POST', path + '/doc', {'title': 'title1 test', 'body': 'body1', 'url': 'url1'})
            api('POST', path + '/doc', [
                {'title': 'title1 test', 'body': 'body1', 'url': 'url1'},
                {'title': 'title2', 'body': 'body2 test', 'url': 'url2'},
                {'title': 'title3 test', 'body': 'body3 test', 'url': 'url3'}])
            check(api('PATCH', path) == 4, 'commit must expose four documents')
            for method in ('GET', 'POST', 'PUT'):
                request(method, path + '/synonyms', key=api_key, expected=404)
            for field, value in [('stemmer', 'None'), ('stemmer', 'English'), ('synonyms', []), ('synonyms', None)]:
                request('POST', '/api/v1/index', {'index_name': 'retired', 'schema': schema, field: value},
                        api_key, expected=400)
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
                    key=api_key, expected=405)
            literal = api('POST', path + '/query', {'query': 'tset'})
            check(literal['query'] == literal['original_query'] == 'tset' and
                  literal['count_total'] == 0 and 'suggestions' not in literal,
                  f'query unexpectedly rewritten: {literal}')
            print('PASS retired query options rejected and literal query preserved')
            request('POST', '/api/v1/index', {'index_name': 'geo', 'schema': [{'field': 'location', 'field_type': 'Point', 'store': True, 'index_lexical': False, 'filterable': True}]}, api_key, expected=400)
            geo_options = [
                {'distance_fields': []},
                {'filter': [{'Point': {'field': 'location', 'filter': [[0, 0], {'start': 0, 'end': 10}, 'Kilometers']}}]},
                {'query_facets': [{'Point': {'field': 'location', 'base': [0, 0], 'unit': 'Kilometers',
                    'range_type': 'CountWithinRange', 'ranges': [['near', 0]]}}]},
                {'sort': [{'field': 'location', 'order': 'Ascending', 'base': {'Point': [0, 0]}}]},
                {'sort': [{'field': 'date', 'order': 'Ascending', 'base': 'None'}]},
            ]
            for options in geo_options:
                for method in ('GET', 'POST'):
                    request(method, path + '/query', {'query': 'test', **options}, api_key, expected=405 if method == 'GET' else 400)
                request('GET', path + '/query?' + urlencode({'query': 'test',
                    **{name: json.dumps(value) for name, value in options.items()}}), key=api_key, expected=405)
            request('GET', path + '/doc/0', {'distance_fields': []}, api_key, expected=400)
            request('GET', path + '/doc/0?distance_fields=%5B%5D', key=api_key, expected=400)
            check(api('POST', path + '/query', {'query': 'test', 'length': 10})['count_total'] == 2,
                  'retained POST JSON query options failed')
            print('PASS retired geographic schema/query/read options rejected by HTTP')

            result = api('POST', path + '/query', {'query': '+body2 +test'})
            check(result['count_total'] == 1 and len(result['results']) == 1,
                  f'lexical query mismatch: {result}')
            doc_id = result['results'][0]['_id']
            document = api('GET', path + f'/doc/{doc_id}?fields=body')
            check(document == {'body': 'body2 test'}, f'stored document mismatch: {document}')
            for highlights in ([], [{'field': 'body', 'highlight_markup': True}]):
                for method in ('GET', 'POST'):
                    request(method, path + '/query', {'query': 'test', 'highlights': highlights}, api_key, expected=405 if method == 'GET' else 400)
                request('GET', path + '/query?' + urlencode({'query': 'test', 'highlights': json.dumps(highlights)}),
                        key=api_key, expected=405)
                request('GET', path + f'/doc/{doc_id}', {'highlights': highlights}, api_key, expected=400)
            for name in ('highlights', 'query_terms'):
                request('GET', path + f'/doc/{doc_id}?' + urlencode({name: '[]'}), key=api_key, expected=400)
            request('GET', path + f'/doc/{doc_id}', {'query_terms': ['test']}, api_key, expected=400)
            check(result['results'][0]['body'] == 'body2 test' and 'body2' not in result['results'][0],
                  'search changed stored text or created a presentation field')
            check(result['query_terms'], 'lexical query terms missing from search response')
            print('PASS retired presentation options rejected; stored fields and response query terms retained')
            check(set(api('GET', path + f'/doc/{doc_id}?fields=body')) == {'body'}, 'URL projection')
            request('GET', path + '/iterator?unknown=true', key=api_key, expected=405)
            request('GET', path + '/iterator?document_id=bad', key=api_key, expected=405)
            request('POST', path + '/iterator', {'unknown': True}, api_key, expected=400)
            request('POST', path + '/iterator', {'document_id': 'bad'}, api_key, expected=400)
            first_page = api('POST', path + '/iterator', {'take': 2, 'include_document': True, 'fields': ['body']})
            check(len(first_page['results']) == 2, f'POST iterator first page: {first_page}')
            check(all(set(item['doc']) == {'body'} for item in first_page['results']),
                  f'POST iterator projection: {first_page}')
            next_page = api('POST', path + '/iterator', {'document_id': first_page['results'][-1]['doc_id'],
                            'skip': 1, 'take': 2, 'include_document': True})
            check(len(next_page['results']) == 2 and
                  next_page['results'][0]['doc_id'] > first_page['results'][-1]['doc_id'],
                  f'POST iterator next page: {next_page}')
            last_page = api('POST', path + '/iterator', {'take': -2})
            check([item['doc_id'] for item in last_page['results']] ==
                  [item['doc_id'] for item in reversed(next_page['results'])],
                  f'POST iterator reverse traversal: last={last_page}, next={next_page}')
            for suffix, body in [('/query', {'query': 'test'}), ('/iterator', {'take': 1})]:
                for method in ('GET', 'QUERY'):
                    for ending in ('', '/', '?query=test'):
                        request(method, path + suffix + ending, body, api_key, expected=405,
                                expected_headers={'Allow': 'POST, OPTIONS'})
                request('POST', path + suffix, body, expected=401)
                request('POST', path + suffix, b'invalid JSON', api_key, expected=400)
                request('POST', path + suffix + '?ignored=true', body, api_key, expected=400)
                request('OPTIONS', path + suffix, expected=204,
                        expected_headers={'Access-Control-Allow-Methods': 'POST, OPTIONS'})
            print('PASS POST-only search/iterator contracts, authentication, pagination and projection')
            print('PASS explicit document/iterator option handling')

            # The retired file endpoints must fail without adding a record.
            request('POST', path + '/file', b'%PDF-1.7 removed', api_key, expected=404)
            request('GET', path + '/file/0', key=api_key, expected=404)
            check(api('PATCH', path) == 4, 'retired PDF endpoint mutated index')
            check(not list((workdir / 'index').rglob('files')), 'created PDF attachment directory')
            # Keep update/delete/get covered when the core attachment parameter is removed.
            api('PATCH', path + '/doc', {'id': doc_id, 'document': {'body': 'updated unique_record'}})
            check(api('PATCH', path) == 5, 'update did not finish')
            updated = api('POST', path + '/query', {'query': 'unique_record'})
            check(updated['count_total'] == 1, 'updated record not searchable')
            updated_id = updated['results'][0]['_id']
            check(api('GET', path + f'/doc/{updated_id}')['body'] == 'updated unique_record',
                  'updated record not readable')
            api('DELETE', path + f'/doc/{updated_id}')
            check(api('POST', path + '/query', {'query': 'unique_record'})['count_total'] == 0, 'deleted record still visible')
            print('PASS JSON create/write/commit/query/get/update/delete; PDF endpoints removed')

            vector_schema = [schema[1], {'field': 'category', 'field_type': 'String16', 'store': True, 'index_lexical': False, 'filterable': True},
                             {'field': 'date', 'field_type': 'Timestamp', 'store': True, 'index_lexical': False, 'filterable': True},
                             {'field': 'price', 'field_type': 'F64', 'store': True, 'index_lexical': False, 'filterable': True},
                             {'field': 'tags', 'field_type': 'StringSet32', 'store': True, 'index_lexical': False, 'filterable': True}, {'field': 'vector', 'field_type': 'Json', 'store': True,
                                         'index_lexical': False, 'index_vector': True}]
            config = {'index_name': 'external_vectors', 'schema': vector_schema, 'vector': {'dimensions': 3, 'precision': 'F32', 'quantization': 'None', 'metric': 'Cosine'}}
            vector_id = api('POST', '/api/v1/index', config)
            vp = f'/api/v1/index/{vector_id}'
            for invalid in ({'body': 'missing'}, {'body': 'bad', 'vector': [1, 2]},
                            {'body': 'zero', 'vector': [0, 0, 0]},
                            [{'body': 'partial', 'vector': [1, 0, 0]}, {'body': 'missing'}]):
                request('POST', vp + '/doc', invalid, api_key, expected=400)
            check(api('PATCH', vp) == 0, 'invalid batch partially indexed')
            api('POST', vp + '/doc', [{'body': 'alpha', 'vector': [1, 0, 0], 'category': 'a', 'date': 1609459200, 'price': 10, 'tags': ['red', 'green']},
                                     {'body': 'beta', 'vector': [0, 1, 0], 'category': 'b', 'date': 1640995200, 'price': 20, 'tags': ['blue']}])
            check(api('PATCH', vp) == 2, 'external write count')
            for mode in ('Vector', 'Hybrid'):
                query_request = {'query': '' if mode == 'Vector' else 'alpha'}
                for invalid in ([1], [0, 0, 0], ['x', 0, 0], '!base64',
                                base64.b64encode(bytes.fromhex('7f8000000000000000000000')).decode()):
                    request('POST', vp + '/query', {**query_request, 'query_vector': invalid},
                            api_key, expected=400)
                result = api('POST', vp + '/query', {**query_request, 'query_vector': [1, 0, 0]})
                check(result['results'][0]['body'] == 'alpha', f'{mode} ranking: {result}')
                for predicate in [
                    {'String16': {'field': 'category', 'filter': ['b']}},
                    {'Timestamp': {'field': 'date', 'filter': {'start': 1640995200, 'end': 1672531200}}},
                    {'F64': {'field': 'price', 'filter': {'start': 20, 'end': 30}}},
                    {'StringSet32': {'field': 'tags', 'filter': ['blue']}}
                ]:
                    filtered_vector = api('POST', vp + '/query', {**query_request, 'query_vector': [1, 0, 0],
                        'length': 1, 'filter': [predicate]})
                    check([doc['body'] for doc in filtered_vector['results']] == ['beta'], f'{mode} filtered Top-1')
                absent = api('POST', vp + '/query', {**query_request, 'query_vector': [1, 0, 0],
                    'filter': [{'String16': {'field': 'category', 'filter': ['absent']}}]})
                check(absent['results'] == [], f'{mode} absent metadata')
            for name in ('Lexical', 'Vector', 'Hybrid'):
                for predicate in [
                    {'String16': {'field': 'missing', 'filter': ['b']}},
                    {'String16': {'field': 'body', 'filter': ['beta']}},
                    {'F64': {'field': 'category', 'filter': {'start': 0, 'end': 1}}},
                    {'Timestamp': {'field': 'date', 'filter': {'start': 2, 'end': 1}}}
                ]:
                    rejected = request('POST', vp + '/query', {'query': '' if name == 'Vector' else 'alpha', 'query_vector': None if name == 'Lexical' else [1, 0, 0], 'filter': [predicate]}, api_key, expected=400)
                    check(b'filter' in rejected, f'{name} invalid metadata predicate accepted')
            request('DELETE', vp + '/doc', {'query': 'alpha', 'filter': [
                {'String16': {'field': 'missing', 'filter': ['a']}}]}, api_key, expected=400)
            print('PASS metadata filters before vector/hybrid Top-K; invalid predicates fail across modes')
            filtered = api('POST', vp + '/query', {'query': '', 'filter': [{'String16': {'field': 'category', 'filter': ['b']}}]})
            check([doc['body'] for doc in filtered['results']] == ['beta'], 'lexical metadata filter')
            request('PATCH', vp + '/doc', {'id': 0, 'document': {'body': 'invalid update'}}, api_key, expected=400)
            request('PATCH', vp + '/doc', [{'id': 0, 'document': {'body': 'changed', 'vector': [0, 0, 1]}},
                                           {'id': 1, 'document': {'body': 'invalid'}}], api_key, expected=400)
            lexical = api('POST', vp + '/query', {'query': 'alpha'})
            check(lexical['results'][0]['body'] == 'alpha', 'failed update lost original')
            for method in ('GET', 'POST', 'QUERY', 'OPTIONS'):
                retired_response = request(method, f'/api/v2/index/{vector_id}/query', b'retired binary payload', api_key,
                                           expected=204 if method == 'OPTIONS' else 404)
                unknown_response = request(method, '/unknown-route', b'retired binary payload', api_key,
                                           expected=204 if method == 'OPTIONS' else 404)
                check(retired_response == unknown_response, 'retired path still has a dedicated response')
            print('PASS retired binary query protocol is unavailable')
            for vector in ({'dimensions': 0}, {'dimensions': 3, 'metric': 'invalid'},
                           {'dimensions': 3, 'precision': 'None'},
                           {'dimensions': 3, 'precision': 'I8', 'quantization': 'TurboQuantI8'}):
                request('POST', '/api/v1/index', {**config, 'vector': vector}, api_key, expected=400)
            binary_schema = [{**field, 'field_type': 'Binary'} if field['field'] == 'vector' else field
                             for field in vector_schema]
            binary_id = api('POST', '/api/v1/index', {**config, 'schema': binary_schema})
            bp = f'/api/v1/index/{binary_id}'
            encoded = base64.b64encode(bytes.fromhex('3f8000000000000000000000')).decode()
            api('POST', bp + '/doc', {'body': 'binary', 'vector': encoded})
            check(api('PATCH', bp) == 1, 'binary vector write')
            result = api('POST', bp + '/query', {'query': '', 'query_vector': encoded})
            check(result['results'][0]['body'] == 'binary', 'base64 vector query')
            print('PASS external vector/hybrid, invalid input, batch and update atomic validation')
            metadata_id = api('POST', '/api/v1/index', {'index_name': 'metadata', 'schema': [{**field, 'store': True} for field in schema] + [{'field': 'category', 'field_type': 'String16', 'store': True, 'index_lexical': False, 'filterable': True}, {'field': 'price', 'field_type': 'F64', 'store': True, 'index_lexical': False, 'filterable': True}, {'field': 'tags', 'field_type': 'StringSet32', 'store': True, 'index_lexical': False, 'filterable': True}, {'field': 'date', 'field_type': 'Timestamp', 'store': True, 'index_lexical': False, 'filterable': True}]})
            mp = f'/api/v1/index/{metadata_id}'
            api('POST', mp + '/doc', [
                {'title': 'Old record', 'body': 'ordinary metadata', 'url': '', 'category': 'a',
                 'tags': ['red'], 'price': 10, 'date': 1590969600},
                {'title': 'New record', 'body': 'ordinary metadata', 'url': '', 'category': 'b',
                 'tags': ['blue'], 'price': 20, 'date': 1654041600}])
            api('PATCH', mp)
            for filter_value in [
                {'String16': {'field': 'category', 'filter': ['b']}},
                {'StringSet32': {'field': 'tags', 'filter': ['blue']}},
                {'F64': {'field': 'price', 'filter': {'start': 20, 'end': 30}}},
                {'Timestamp': {'field': 'date', 'filter': {'start': 1640995200, 'end': 1672531200}}},
            ]:
                filtered = api('POST', mp + '/query', {'query': 'ordinary', 'filter': [filter_value]})
                check(filtered['count_total'] == 1 and filtered['results'][0]['title'] == 'New record',
                      f'ordinary metadata filter changed: {filtered}')
            for order, titles in [('Ascending', ['Old record', 'New record']), ('Descending', ['New record', 'Old record'])]:
                sorted_result = api('POST', mp + '/query', {'query': 'ordinary',
                    'sort': [{'field': 'date', 'order': order}]})
                check([doc['title'] for doc in sorted_result['results']] == titles, 'ordinary date sorting')
            for retired_facets in [[], [{'String16': {'field': 'category', 'prefix': '', 'length': 10}}], [{'F64': {'field': 'price', 'range_type': 'CountWithinRange', 'ranges': [['low', 0]]}}]]:
                rejected = request('POST', mp + '/query', {'query': 'ordinary', 'query_facets': retired_facets}, api_key, expected=400)
                check(b'query_facets' in rejected, 'retired aggregation was not rejected')
            check('facets_minmax' not in api('GET', mp), 'retired min/max present')
            check('facets' not in api('POST', mp + '/query', {'query': 'ordinary'}), 'retired counts present')
            print('PASS retired aggregates rejected; ordinary string/set/number/timestamp filters and date sorting')
            raw_id = api('POST', '/api/v1/index', {'index_name': 'raw_records', 'schema': [
                {**field, 'store': True} for field in schema] + [
                {'field': 'data', 'field_type': 'Json', 'store': True, 'index_lexical': True},
                {'field': 'body2', 'field_type': 'Text', 'store': True, 'index_lexical': False}]})
            rp = f'/api/v1/index/{raw_id}'
            raw_record = {'title': '<b>Literal title 中文</b>',
                'body': 'rawmarker <img src=x onerror="window.rawExecuted=true"> & 中文\r\n第二行\n' + '完整原文 ' * 1000 + 'END',
                'url': 'javascript:window.rawExecuted=true',
                'data': {'nested': ['<em>中文</em>', 0, False, None], 'line': 'a\nb'},
                'body2': 'ordinary stored field, never the preview source'}
            api('POST', rp + '/doc', raw_record)
            api('PATCH', rp)
            raw_result = api('POST', rp + '/query', {'query': 'rawmarker'})
            check(raw_result['count_total'] == 1, 'raw text not searchable')
            raw_doc = raw_result['results'][0]
            raw_doc_id = raw_doc['_id']
            check({k: v for k, v in raw_doc.items() if k not in ('_id', '_score')} == raw_record,
                  'search altered stored text or nested JSON')
            check(api('GET', rp + f'/doc/{raw_doc_id}') == raw_record, 'raw document changed')
            check(api('GET', rp + f'/doc/{raw_doc_id}?fields=data,missing') == {'data': raw_record['data']},
                  'projection changed nested JSON or returned missing fields')
            projected = api('POST', rp + '/query', {'query': 'rawmarker', 'fields': ['body', 'data']})['results'][0]
            check({k: v for k, v in projected.items() if k not in ('_id', '_score')} ==
                  {k: raw_record[k] for k in ('body', 'data')}, 'search projection changed stored values')
            print('PASS exact raw Unicode/multiline/HTML-like text, nested JSON and field projection')
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
                    from setup_browser_smoke import exercise_setup
                    exercise_setup(browser, base, master)
                    page = browser.new_page()
                    errors = []
                    page.on('pageerror', lambda error: (errors.append(str(error)), print(f'BROWSER ERROR: {error}')))
                    # URL credentials must not authenticate a fresh browser session.
                    from playwright.sync_api import expect
                    page.goto(base + '/?' + urlencode({'api_key': api_key, 'index_id': vector_id}))
                    expect(page.locator('#welcome')).to_be_visible()
                    check(page.evaluate("sessionStorage.getItem('seekpower.apiKey')") is None,
                          'URL credential was persisted')
                    check(page.evaluate('API_KEY') == '', 'URL credential authenticated the UI')
                    page.locator('#setupKey').fill(api_key)
                    page.locator('#connectForm button[type="submit"]').click()
                    expect(page.locator('#openIndex')).to_be_enabled()
                    page.goto(base + '/?' + urlencode({'index_id': vector_id, 'q': 'alpha'}))
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
                    check(typed_response.request.headers.get('content-type', '').startswith('application/json'),
                          'JSON query sent with an incorrect content type')
                    check('query_rewriting' not in typed_response.request.post_data_json,
                          'UI sent retired rewriting parameter')
                    check(typed_response.json()['count_total'] == 0, 'typo silently corrected')
                    check(search_input.input_value() == 'plaintxt', 'typed input was rewritten')
                    check(page.locator('#suggestions, #suggestionsMain, #did_you_mean').count() == 0,
                          'retired suggestions UI remains')
                    with page.expect_response(lambda response: '/query' in response.url and
                            response.request.method == 'POST') as raw_response:
                        page.goto(base + f'/?index_id={raw_id}&q=rawmarker')
                    response = raw_response.value
                    check(response.status == 200 and 'highlights' not in response.request.post_data_json,
                          'UI sent retired presentation options')
                    page.locator('#contentBox .recordText').first.wait_for()
                    check(page.locator('#contentBox h1 a').first.text_content() == raw_record['title'], 'UI title modified')
                    check(page.locator('#contentBox .recordText').first.text_content() == raw_record['body'], 'UI body truncated or modified')
                    check(page.locator('#contentBox a[href]').count() == 0, 'non-HTTP stored URL became an active link')
                    page.locator('#contentBox .resultEntry h1').first.hover()
                    check(page.locator('#prevText2').text_content() == raw_record['body'], 'full raw preview changed or used body2')
                    check(page.locator('#prevTitle').text_content() == raw_record['title'], 'preview title modified')
                    check(page.locator('#contentBox b, #contentBox img, #prevText2 img, #prevTitle b').count() == 0,
                          'stored markup became HTML elements')
                    check(page.evaluate('window.rawExecuted !== true'), 'stored markup executed')
                    check(page.locator('#prevText2').evaluate('(el) => getComputedStyle(el).whiteSpace') == 'pre-wrap',
                          'preview loses raw line breaks')
                    check(page.locator('#prevText2').evaluate('(el) => el.scrollHeight > el.clientHeight'),
                          'long raw preview has no scrollable viewport')
                    page.locator('#prevText2').evaluate('(el) => { el.scrollTop = el.scrollHeight; }')
                    check(page.locator('#prevText2').evaluate('(el) => el.scrollTop > 0'), 'long raw preview cannot scroll')
                    # JSON fields use readable JSON text, including false and zero values.
                    with page.expect_response(lambda response: '/query' in response.url and response.request.method == 'POST'):
                        page.evaluate("TEXT_FIELD='data'; loadResults(1, 'score')")
                    expected_json = json.dumps(raw_record['data'], ensure_ascii=False, separators=(',', ':'))
                    page.wait_for_function('(value) => document.querySelector(".recordText").textContent === value', arg=expected_json)
                    page.locator('#contentBox .resultEntry h1').first.hover()
                    check(page.locator('#prevText2').text_content() == expected_json, 'nested JSON preview lost types')
                    page.goto(base + f'/?index_id={metadata_id}&q=ordinary')
                    page.locator('#new').wait_for()
                    for selector, title in [('#new', 'New record'), ('#old', 'Old record')]:
                        with page.expect_response(lambda response: '/query' in response.url and
                                response.request.method == 'POST') as sorted_response:
                            page.locator(selector).click()
                        response = sorted_response.value
                        check(response.status == 200, 'UI date sorting request failed')
                        check(all('base' not in item for item in response.request.post_data_json['sort']),
                              'UI sent retired sort base')
                        check(response.json()['results'][0]['title'].startswith(title), 'UI date ordering changed')
                    page.locator('#dateFrom').fill('2022-01-01')
                    page.locator('#dateTo').fill('2022-12-31')
                    with page.expect_response(lambda response: '/query' in response.url and
                            response.request.method == 'POST') as filtered_response:
                        page.locator('#dateApply').click()
                    response = filtered_response.value
                    check(response.status == 200 and response.json()['count_total'] == 1,
                          'UI timestamp filter failed')
                    check(response.json()['results'][0]['title'].startswith('New record'), 'UI timestamp result changed')
                    with page.expect_response(lambda response: '/query' in response.url and
                            response.request.method == 'POST') as cleared_response:
                        page.locator('#dateClear').click()
                    response = cleared_response.value
                    check(response.status == 200 and response.json()['count_total'] == 2, 'UI clear dates failed')
                    check(response.request.post_data_json['filter'] == [], 'UI kept cleared date filter')
                    page.goto(base + f'/?index_id={vector_id}&q=beta')
                    page.locator('#dateApply').wait_for()
                    page.locator('#dateFrom').fill('2022-01-01')
                    page.locator('#dateTo').fill('2022-12-31')
                    for selector in ('#dateApply', '#vectorSearch', 'label[data-index="1"]'):
                        if selector == '#vectorSearch':
                            page.locator('label[data-index="0"]').click()
                            page.locator('#queryVector').fill('[1,0,0]')
                        with page.expect_response(lambda response: '/query' in response.url and response.request.method == 'POST') as mode_filter:
                            page.locator(selector).click()
                        response = mode_filter.value
                        check(response.status == 200 and [doc['body'] for doc in response.json()['results']] == ['beta'], 'UI shared date filtering failed')
                    check(not errors, f'browser errors: {errors}')
                    browser.close()
                print('PASS Chromium lexical/vector/hybrid, source/text preview, literal raw/JSON preview, unchanged typed query and date sorting/filtering')

            # Removed close/version routes follow the existing unknown-route response.
            request('PUT', path, key=api_key, expected=405)
            api('POST', path + '/query', {'query': 'body1'})
            # Both process signals must commit accepted writes without an explicit PATCH.
            shutdown_id = api('POST', '/api/v1/index', {'index_name': 'shutdown', 'schema': schema})
            shutdown_path = f'/api/v1/index/{shutdown_id}'
            for signum in (signal.SIGTERM, signal.SIGINT):
                marker = f'shutdownsignal{signum}'
                api('POST', shutdown_path + '/doc', {'body': marker})
                process.send_signal(signum)
                process.wait(timeout=30)
                check(process.returncode == 0, f'unclean signal shutdown: {signum}')
                process = launch()
                ready(process)
                result = api('POST', shutdown_path + '/query', {'query': marker})
                check(result['count_total'] == 1, f'shutdown lost accepted write: {signum}')
            api('DELETE', shutdown_path)
            print('PASS removed close/version routes and SIGTERM/SIGINT commit before exit')

            # Public contracts cover batch replacement, strict deletion and all matches (not a page).
            unified_id = api('POST', '/api/v1/index', {'index_name': 'unified', 'schema': [
                {'field': 'body', 'field_type': 'Text'},
                {'field': '类别', 'field_type': 'String16', 'filterable': True}]})
            up = f'/api/v1/index/{unified_id}'
            info = api('GET', up)
            check(isinstance(info['schema'], list) and
                  all(set(f) == {'field', 'field_type', 'store', 'index_lexical', 'index_vector', 'filterable', 'boost'} for f in info['schema']), 'public schema leaked engine fields')
            check(not {'operations_count', 'query_count'} & set(info), 'placeholder counters exposed')
            api('POST', up + '/doc', [{'body': 'match', '类别': 'remove'} for _ in range(12)] +
                                    [{'body': 'match', '类别': 'keep'}])
            check(api('GET', up + '/doc/0?' + urlencode({'fields': '类别'})) == {'类别': 'remove'}, 'encoded URL projection')
            for body in (0, [0, 1], b'clear', {'all': False}, {'all': True, 'query': 'match'},
                         {'ids': [0], 'query': 'match'}, {'query': 'match', 'length': 1},
                         {'query': 'match', 'sort': []}, {'query': 'match', 'fields': ['body']},
                         {'query': 'match', 'count': False}, {'query': 'match', 'filter': [
                             {'String16': {'field': 'missing', 'filter': ['remove']}}]}):
                request('DELETE', up + '/doc', body, api_key, expected=400)
            for suffix, body in [('/doc/bad', None), ('/doc/-1', None), ('/doc/0', {'all': True})]:
                request('DELETE', up + suffix, body, api_key, expected=400)
            request('GET', up + '/doc/0', {'fields': ['body']}, api_key, expected=400)
            request('GET', up + '/doc/0?fields=body&fields=other', key=api_key, expected=400)
            for old in ('facet_filter', 'result_sort', 'field_filter'):
                request('POST', up + '/query', {old: []}, api_key, expected=400)
            check(api('POST', up + '/query', {})['count_total'] == 13, 'rejected deletion mutated records')
            api('DELETE', up + '/doc', {'query': 'match', 'search_fields': ['body'],
                'filter': [{'String16': {'field': '类别', 'filter': ['remove']}}]})
            check(api('POST', up + '/query', {})['count_total'] == 1, 'delete query did not remove all 12 pending matches')
            request('PATCH', up + '/doc', [12, {'body': 'old tuple'}], api_key, expected=400)
            api('PATCH', up + '/doc', {'id': 12, 'document': {'body': 'replacement'}})
            replacement = api('POST', up + '/query', {'query': 'replacement'})['results'][0]
            check(replacement['_id'] != 12 and '类别' not in replacement, 'replacement must get a new ID and omit old fields')
            api('POST', up + '/doc', {'body': 'second'})
            second = api('POST', up + '/query', {'query': 'second'})['results'][0]['_id']
            api('PATCH', up + '/doc', [{'id': replacement['_id'], 'document': {'body': 'batchone'}},
                                     {'id': second, 'document': {'body': 'batchtwo'}}])
            ids = [d['_id'] for d in api('POST', up + '/query', {})['results']]
            check(len(ids) == 2, 'batch replacement count')
            api('DELETE', up + '/doc', {'ids': ids})
            check(api('POST', up + '/query', {})['count_total'] == 0, 'delete IDs')
            api('DELETE', up)
            print('PASS public schema, URL projection, named batch replacements, strict JSON deletion and all-match deletion')

            # Storage failures are 500 JSON errors, not 400, success, or dropped connections.
            occupied = {index['id'] for index in api('GET', '/api/v1/apikey')}
            next_id = next(i for i in range(len(occupied) + 1) if i not in occupied)
            obstacle = workdir / 'index' / '1' / str(next_id)
            obstacle.write_text('block directory creation')
            request('POST', '/api/v1/index', {'index_name': 'blocked', 'schema': schema}, api_key, expected=500)
            obstacle.unlink()
            check({index['id'] for index in api('GET', '/api/v1/apikey')} == occupied, 'failed create published index')
            blocked_key = workdir / 'index' / '2'
            blocked_key.mkdir()
            (blocked_key / 'apikey.bak').mkdir()
            request('POST', '/api/v1/apikey', {}, master, expected=500)
            (blocked_key / 'apikey.bak').rmdir()
            blocked_key.rmdir()
            check(api('GET', path)['id'] == index_id, 'failed key persistence affected existing scope')
            for invalid_id in ('bad', '-1', '%FF'):
                for method in ('GET', 'DELETE'):
                    request(method, '/api/v1/index/' + invalid_id, key=api_key, expected=400)
                request('POST', '/api/v1/index/' + invalid_id + '/query', {}, api_key, expected=400)
            request('HEAD', '/api/v1/live', expected=200)
            request('OPTIONS', path, expected=204, expected_headers={
                'Access-Control-Allow-Methods': 'GET, HEAD, POST, PATCH, DELETE, OPTIONS'})
            print('PASS storage failures return structured 500 without publishing failed resources')

            # Minimal public settings resolve once at creation and survive restart.
            defaults = {'index_name': 'defaults', 'schema': [
                {'field': 'body', 'field_type': 'Text'},
                {'field': 'category', 'field_type': 'String16', 'filterable': True},
                {'field': 'vector', 'field_type': 'Json', 'index_vector': True}],
                'vector': {'dimensions': 3}}
            default_id = api('POST', '/api/v1/index', defaults)
            dp = f'/api/v1/index/{default_id}'
            effective = {field['field']: field for field in api('GET', dp)['schema']}
            check(effective['body']['store'] and effective['body']['index_lexical'], 'text defaults')
            check(effective['category']['store'] and effective['category']['filterable'] and
                  not effective['category']['index_lexical'], 'metadata defaults')
            check(not effective['vector']['store'] and not effective['vector']['index_lexical'], 'vector defaults')
            metadata_path = workdir / 'index' / '1' / str(default_id) / 'index.json'
            metadata = metadata_path.read_bytes()
            settings = json.loads(metadata)
            check(settings['document_compression'] == 'Snappy' and settings['ngram_indexing'] == 9 and
                  settings['frequent_words'] == 'English' and settings['clustering'] == 'Auto' and
                  settings['lexical_similarity'] == 'Bm25fProximity', 'managed creation settings')
            check(settings['inference']['External'] == {'dimensions': 3, 'precision': 'F32',
                  'quantization': 'None', 'similarity': 'Cosine'}, 'vector defaults')
            api('POST', dp + '/doc', [{'body': 'alpha', 'category': 'a', 'vector': [1, 0, 0]},
                                     {'body': 'beta', 'category': 'b', 'vector': [0, 1, 0]}])
            for payload in ({'query': 'alpha'}, {'query_vector': [1, 0, 0]},
                            {'query': 'alpha', 'query_vector': [1, 0, 0]}):
                result = api('POST', dp + '/query', payload)
                check(result['results'][0]['body'] == 'alpha' and 'vector' not in result['results'][0],
                      f'inferred mode/default projection before commit: {payload}')
            check(api('POST', dp + '/query', {})['count_total'] == 2, 'empty input must browse')
            check(api('POST', dp + '/query', {'query': 'alpha beta'})['count_total'] == 0, 'default AND')
            check(api('POST', dp + '/query', {'query': 'alpha beta', 'operator': 'or'})['count_total'] == 2, 'explicit OR')
            count_only = api('POST', dp + '/query', {'length': 0})
            check(count_only['count_total'] == 2 and count_only['results'] == [], 'count-only search')
            for payload in ({}, {'query_vector': [1, 0, 0]}, {'query': 'alpha', 'query_vector': [1, 0, 0]}):
                if 'query_vector' in payload:
                    request('POST', dp + '/query', {**payload, 'length': 0}, api_key, expected=400)
                uncounted = api('POST', dp + '/query', {**payload, 'count': False, 'length': 1})
                check(uncounted['count_total'] is None and len(uncounted['results']) == 1, 'uncounted search')
            iterator = api('POST', dp + '/iterator', {})
            check(len(iterator['results']) == 2 and all(item['doc']['body'] for item in iterator['results']),
                  'iterator defaults must include live documents')
            reverse = api('POST', dp + '/iterator', {'take': -10, 'fields': ['body']})
            check([item['doc_id'] for item in reverse['results']] ==
                  [item['doc_id'] for item in reversed(iterator['results'])] and
                  all(set(item['doc']) == {'body'} for item in reverse['results']), 'reverse iterator projection')
            for body in ({}, {'query': ''}, {'query': 'alpha', 'query_vector': [1, 0, 0]}):
                request('DELETE', dp + '/doc', body, api_key, expected=400)
            for key, value in {'similarity': 'Bm25f', 'frequent_words': 'None', 'ngram_indexing': 0,
                               'document_compression': 'None', 'clustering': 'None', 'inference': 'None'}.items():
                request('POST', '/api/v1/index', {**defaults, key: value}, api_key, expected=400)
            for key, value in {'search_mode': 'Lexical', 'realtime': False, 'enable_empty_query': False,
                               'result_type': 'Count', 'query_type_default': 'Union', 'ann_mode': 'All'}.items():
                request('POST', dp + '/query', {'query': 'alpha', key: value}, api_key, expected=400)
            for key in ('longest', 'facet', 'field_id', 'indexed_field_id'):
                request('POST', '/api/v1/index', {**defaults, 'schema': [
                    {'field': 'body', 'field_type': 'Text', key: True}]}, api_key, expected=400)
            for fields in ([], [{'field': '', 'field_type': 'Text'}],
                           [{'field': 'body', 'field_type': 'Text'}] * 2):
                request('POST', '/api/v1/index', {**defaults, 'schema': fields}, api_key, expected=400)
            request('POST', dp + '/iterator', {'include_deleted': True}, api_key, expected=400)
            request('POST', dp + '/query', {'similarity_threshold': 0.5}, api_key, expected=400)
            api('PATCH', dp)
            process.terminate()
            process.wait(timeout=10)
            process = launch()
            ready(process)
            check(metadata_path.read_bytes() == metadata, 'restart rewrote stored creation settings')
            check(api('POST', dp + '/query', {'query_vector': [1, 0, 0]})['results'][0]['body'] == 'alpha',
                  'simplified vector index did not survive restart')
            api('DELETE', dp)
            print('PASS minimal schema, managed settings, inferred modes, default visibility, count/iterator and strict parameters')

            for codec in ('Lz4', 'Zstd'):
                request('POST', '/api/v1/index', {'index_name': 'retired_codec', 'schema': schema,
                        'document_compression': codec}, api_key, expected=400)
            print('PASS removed document codecs rejected by HTTP')

            # HTTP uses managed N-gram defaults; the Rust core still tests every mask.
            ng_id = api('POST', '/api/v1/index', {'index_name': 'ngram_defaults', 'schema': schema})
            ng_path = f'/api/v1/index/{ng_id}'
            api('POST', ng_path + '/doc', [{'body': 'of the art'}, {'body': 'of art the'}, {'body': 'rust'}])
            for phrase in ('of the art', 'of art the'):
                result = api('POST', ng_path + '/query', {'query': f'"{phrase}"'})
                check(result['count_total'] == 1 and result['results'][0]['body'] == phrase,
                      f'Default N-grams changed phrase matches: {phrase}')
            for query in ('r', 'ru', 'us', 'r*'):
                check(api('POST', ng_path + '/query', {'query': query})['count_total'] == 0,
                      'word N-grams must not introduce character prefix/substring search')
            api('DELETE', ng_path)
            print('PASS managed N-gram defaults and word-level query contract')

            # Key creation, revocation and scopes remain independent of retired quotas.
            def scoped(key, method, path, data=None):
                return json.loads(request(method, path, data, key))

            key_file = workdir / 'index' / '1' / 'apikey.json'
            key_bytes = key_file.read_bytes()
            key_record = json.loads(key_bytes)
            check(set(key_record) == {'id', 'apikey_hash'}, 'new key persisted quota or runtime scope')
            original_doc = api('GET', path + '/doc/0')
            other_key = json.loads(request('POST', '/api/v1/apikey', {}, master))
            check(other_key != api_key and len(base64.b64decode(other_key)) == 32, 'random API key invalid')
            check(key_file.read_bytes() == key_bytes and api('GET', path + '/doc/0') == original_doc,
                  'creating a second key changed the existing identity or scope')
            other_id = scoped(other_key, 'POST', '/api/v1/index', {'index_name': 'isolated', 'schema': schema})
            check(other_id == 0, 'new key should have its own index ID namespace')
            other_path = f'/api/v1/index/{other_id}'
            scoped(other_key, 'POST', other_path + '/doc', {'body': 'private_other_key'})
            scoped(other_key, 'PATCH', other_path)

            def assert_scopes():
                check(api('GET', path + '/doc/0') == original_doc, 'first key document changed')
                check(scoped(other_key, 'GET', other_path + '/doc/0') == {'body': 'private_other_key'},
                      'second key document changed')
                check(api('POST', path + '/query', {'query': 'private_other_key'})['count_total'] == 0,
                      'query leaked other key results')
                check(scoped(other_key, 'POST', other_path + '/query', {'query': 'body1'})['count_total'] == 0,
                      'query leaked first key results')
                for method, suffix, body in [('GET', '', None), ('GET', '/doc/0', None),
                        ('POST', '/query', {'query': 'rawmarker'}),
                        ('POST', '/doc', {'body': 'cross_scope_write'}),
                        ('DELETE', '', None)]:
                    request(method, rp + suffix, body, other_key, expected=404)
                request('GET', rp, key=master, expected=404)
                request('GET', path, key='not-base64!', expected=401)
                request('POST', '/api/v1/apikey', {}, other_key, expected=401)
                request('DELETE', '/api/v1/apikey', {'apikey_base64': api_key}, other_key, expected=401)

            assert_scopes()
            # Simulate an existing installation, keeping its identity and data paths unchanged.
            process.terminate()
            process.wait(timeout=10)
            key_record['quota'] = {'indices_max': 0, 'indices_size_max': 0, 'documents_max': 0,
                                   'operations_max': 0, 'rate_limit': 0, 'demo': True}
            legacy_bytes = json.dumps(key_record).encode()
            key_file.write_bytes(legacy_bytes)
            persisted_files = {str(file.relative_to(workdir / 'index')) for file in (workdir / 'index').rglob('apikey.json')}
            process = launch()
            ready(process)
            assert_scopes()
            check(key_file.read_bytes() == legacy_bytes, 'startup rewrote legacy key identity/configuration')
            check({str(file.relative_to(workdir / 'index')) for file in (workdir / 'index').rglob('apikey.json')} == persisted_files,
                  'restart moved key directories')
            for _ in range(12):
                api('GET', path)
            request('DELETE', '/api/v1/apikey', {'apikey_base64': other_key}, api_key, expected=401)
            request('DELETE', '/api/v1/apikey', {'apikey_base64': other_key}, master)
            request('GET', other_path, key=other_key, expected=401)
            request('POST', other_path + '/doc', {'body': 'revoked'}, other_key, expected=401)
            check(api('GET', path + '/doc/0') == original_doc, 'revocation affected another key')
            check(api('GET', rp + f'/doc/{raw_doc_id}') == raw_record, 'restart changed retained raw records')
            print('PASS strict random key creation, cross-key isolation, legacy restart and revocation')

            check(api('DELETE', path + '/doc', {'all': True}) == 0, 'clear count mismatch')
            check(api('GET', path)['indexed_doc_count'] == 0, 'index not empty after clear')
            print('PASS clear and index info')
            request('DELETE', '/api/v1/apikey', {'apikey_base64': api_key}, master)
            check(list((workdir / 'index').rglob('apikey.json')) == [workdir / 'index' / '0' / 'apikey.json'],
                  'revocation did not leave only the master identity')
            process.terminate()
            process.wait(timeout=10)
            process = launch()
            ready(process)
            request('GET', '/api/v1/apikey', key=api_key, expected=401)
            check(list((workdir / 'index').rglob('apikey.json')) == [workdir / 'index' / '0' / 'apikey.json'],
                  'revoked ordinary keys were regenerated')
            request('GET', '/api/v1/apikey', key=master)
            check((workdir / 'index' / '0' / 'apikey.json').read_bytes() == master_identity,
                  'ordinary key revocation changed the master identity')
            print('PASS ordinary key revocation/restart preserves direct master access')
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
        parser.error(f'server binary not found: {binary}; run cargo build -p seekpower_server')
    with tempfile.TemporaryDirectory(prefix='seekpower-http-') as directory:
        exercise(binary, Path(directory))
    print('PASS HTTP/UI smoke')


if __name__ == '__main__':
    main()
