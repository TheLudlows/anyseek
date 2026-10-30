#!/usr/bin/env python3
"""Focused real-browser i64 editor/write/retry check against a disposable service."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request
import uuid
from playwright.sync_api import sync_playwright, expect


def operation():
    return {'epoch': int(time.time() // 86400), 'nonce': uuid.uuid4().hex}


def exercise(base, master, call, chromium=None):
    collection = 'precision-browser'
    call('POST', '/api/v1/index', {
        'operation_id': operation(), 'collection_id': collection,
        'config': {'index_name': 'Exact integers', 'schema': [
            {'field': 'body', 'field_type': 'Text'},
            {'field': 'rank', 'field_type': 'Int', 'filterable': True},
            {'field': 'measure', 'field_type': 'Float'},
            {'field': 'data', 'field_type': 'Json'}]}})
    values = [-(2**63), 2**63-1, 2**53-1, 2**53, 2**53+1]
    documents = [{'body': 'precision', 'rank': n, 'measure': 1.25,
                  'data': {'n': n, 'text': str(n), 'flags': [True, False, None]}}
                 for n in values]
    raw_docs = [json.dumps(doc).replace('"measure": 1.25', '"measure": 1.2500e0') for doc in documents]
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, **({'executable_path': chromium} if chromium else {}))
        context = browser.new_context()
        page = context.new_page()
        page.add_init_script('sessionStorage.setItem("seekpower.apiKey",'+json.dumps(master)+');sessionStorage.setItem("seekpower.index",'+json.dumps(collection)+');')
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        sent = []

        def intercept(route):
            sent.append(route.request.post_data)
            response = route.fetch()
            assert response.status == 200, response.text()
            if len(sent) == 1:
                route.abort('failed')
            else:
                route.fulfill(response=response)

        page.route('**/api/v1/index/precision-browser/doc', intercept)
        page.goto(base)
        expect(page.locator('#indexName')).to_have_text('Exact integers')
        page.locator('#writeDocuments').click()
        page.locator('#documentJson').fill('[ '+',\n'.join(raw_docs)+' ]')
        page.locator('#submitDocuments').click()
        expect(page.locator('#retryDocumentCommit')).to_be_visible()
        saved = json.loads(page.evaluate('sessionStorage.getItem("seekpower.pendingWrites")'))
        assert saved['requests'][0]['body'] == sent[0]
        for entry, raw in zip(saved['requests'], raw_docs):
            assert entry['body'].endswith('"document":'+raw+'}')
        page.reload()
        expect(page.locator('#indexName')).to_have_text('Exact integers')
        page.locator('#writeDocuments').click()
        page.locator('#retryDocumentCommit').click()
        expect(page.locator('#documentStatus')).to_contain_text('durable and visible')
        assert sent[0] == sent[1], 'retry must use byte-identical body and operation ID'
        requests = [json.loads(raw) for raw in sent[1:]]
        assert len(requests) == len(documents)
        for request, document in zip(requests, documents):
            stored = call('GET', f'/api/v1/index/{collection}/doc/'+request['record_id'])
            assert stored['document'] == document
        # Exercise setupApi's real response parser using current-record reads.
        # Search below separately covers rendering original nested integer tokens.
        for request,document in zip(requests,documents):
            raw=page.evaluate("async id => apiJsonText(await setupApi('GET',`api/v1/index/${INDEX_ID}/doc/${id}`,API_KEY))",request['record_id'])
            assert json.loads(raw)['document']==document
        # Search uses jQuery's JSON converter, not setupApi; exercise its actual DOM.
        page.locator('#backToSearch').click()
        page.evaluate("TITLE_FIELD='rank'; TEXT_FIELD='data'; query='precision'; loadResults(1,'score')")
        expect(page.locator('.recordJson pre')).to_have_count(len(documents))
        copied = page.locator('.recordJson pre').all_text_contents()
        assert sorted(json.loads(raw)['rank'] for raw in copied) == sorted(values)
        assert sorted(int(t) for t in page.locator('.resultEntry h1 a').all_text_contents()) == sorted(values)
        assert sorted(json.loads(t)['n'] for t in page.locator('.recordText').all_text_contents()) == sorted(values)
        # Copy each displayed JSON document into the editor and submit to the service.
        page.locator('#writeDocuments').click()
        page.locator('#documentJson').fill('['+','.join(copied)+']')
        page.locator('#submitDocuments').click()
        expect(page.locator('#documentStatus')).to_contain_text('Wrote 5 record(s)')
        for raw in sent[-len(documents):]:
            request = json.loads(raw)
            stored_copy = call('GET', f'/api/v1/index/{collection}/doc/'+request['record_id'])
            assert stored_copy['document'] == request['document']
            assert stored_copy['document'] in documents
        # A real conditional edit uses the same original-text path as creation.
        record_id = requests[0]['record_id']
        edited = dict(documents[-1], rank=2**63-1)
        envelope = {'record_id': record_id, 'routing_key': record_id,
                    'expected': {'Match': 1}, 'document': edited}
        raw_edit = json.dumps(envelope)
        page.locator('#recordEnvelopeMode').check()
        page.locator('#documentJson').fill(raw_edit)
        page.locator('#submitDocuments').click()
        expect(page.locator('#documentStatus')).to_contain_text('Wrote 1 record(s)')
        assert sent[-1].startswith(raw_edit[:-1]+',"operation_id":')
        stored = call('GET', f'/api/v1/index/{collection}/doc/'+record_id)
        assert stored['version'] == 2 and stored['document'] == edited
        assert not errors, errors
        browser.close()
    print('PASS Chromium: exact i64 creation, response display/copy/edit roundtrip, conditional edit, nested JSON, lost response, reload and byte-identical retry', flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', required=True)
    parser.add_argument('--chromium')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='anyseek-precision-') as directory:
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        base = f'http://127.0.0.1:{port}'
        secret = 'precision-isolated-test'
        master = base64.b64encode(hashlib.sha256(secret.encode()).digest()).decode()
        with open(Path(directory)/'server.log', 'w+') as log:
            process = subprocess.Popen([str(Path(args.server).resolve()), f'index_path={directory}/data', f'local_port={port}', 'local_ip=127.0.0.1'], env={**os.environ, 'MASTER_KEY_SECRET': secret}, stdout=log, stderr=log)
            try:
                deadline = time.monotonic()+30
                while True:
                    try:
                        urllib.request.urlopen(base+'/api/v1/live', timeout=1).close()
                        break
                    except OSError:
                        assert process.poll() is None and time.monotonic()<deadline, 'service failed to start'
                        time.sleep(.05)

                tenant_key=base64.b64encode(hashlib.sha256(b'precision-tenant').digest()).decode()
                def call(method, route, body=None):
                    management=route in ['/api/v1/tenants','/api/v1/apikey'] and method=='POST'
                    key=master if management else tenant_key
                    request = urllib.request.Request(base+route, data=None if body is None else json.dumps(body).encode(), headers={'apikey': key, 'Content-Type': 'application/json'}, method=method)
                    with urllib.request.urlopen(request, timeout=30) as response:
                        return json.load(response)

                from record_service_smoke import operation
                call('POST','/api/v1/tenants',{'operation_id':operation(),'tenant_id':'precision'})
                call('POST','/api/v1/apikey',{'operation_id':operation(),'tenant_id':'precision','key_id':'precision','apikey_base64':tenant_key})
                exercise(base, tenant_key, call, args.chromium)
            except Exception:
                log.flush()
                log.seek(0)
                print(log.read()[-8000:])
                raise
            finally:
                process.kill()
                process.wait(timeout=20)


if __name__ == '__main__':
    main()
