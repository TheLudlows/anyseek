"""Paused background build: real HTTP writes, deletion, revocation and restart."""
import base64
import concurrent.futures
import os
import time
from record_service_smoke import operation


def exercise(call, restart_with, work, faults):
    assert faults, 'Indexer pause requires the explicit fault-injection binary'
    pause = work / 'indexer.pause'
    started = pause.with_suffix('.started')
    pause.write_text('pause until concurrent operations finish')
    try:
        restart_with({'ANYSEEK_INDEXER_PAUSE_FILE': str(pause)})
        key = base64.b64encode(os.urandom(32)).decode()
        call('POST', '/api/v1/apikey', {'operation_id': operation(), 'key_id': 'indexer-key', 'apikey_base64': key})
        collection = 'background'
        prefix = f'/api/v1/index/{collection}'
        call('POST', '/api/v1/index', {'operation_id': operation(), 'collection_id': collection,
             'config': {'index_name': 'background index acceptance', 'schema': [{'field': 'body', 'field_type': 'Text'}]}})
        for identity, system in [('source', {'kind': 'source'}), ('derived', {'kind': 'derived',
             'evidence_refs': [{'record_id': 'source', 'routing_key': 'r', 'version': 1}]})]:
            call('POST', prefix + '/doc', {'operation_id': operation(), 'record_id': identity,
                 'routing_key': 'r', 'expected': 'Absent', 'document': {'body': 'alpha original'}, 'system': system})
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            # The accepted job may complete after revocation; its result remains deliverable.
            job = pool.submit(call, 'PATCH', prefix, key=key)
            try:
                deadline = time.monotonic() + 10
                while not started.exists():
                    assert not job.done(), 'checkpoint returned before the snapshot pause'
                    assert time.monotonic() < deadline, 'Indexer did not fix its input snapshot'
                    time.sleep(.01)
                request = {'operation_id': operation(), 'record_id': 'concurrent', 'routing_key': 'r',
                           'expected': 'Absent', 'document': {'body': 'alpha concurrent'}}
                receipt = pool.submit(call, 'POST', prefix + '/doc', request).result(timeout=5)['receipt']
                deletion = {'operation_id': operation(), 'record_id': 'source', 'routing_key': 'r',
                            'expected': {'Match': 1}, 'document': None}
                pool.submit(call, 'PATCH', prefix + '/doc', deletion).result(timeout=5)
                page = pool.submit(call, 'POST', prefix + '/query', {'query': 'alpha'}).result(timeout=5)
                assert [hit['record']['record_id'] for hit in page['hits']] == ['concurrent'], page
                pool.submit(call, 'DELETE', '/api/v1/apikey', {'operation_id': operation(),
                            'key_id': 'indexer-key'}).result(timeout=5)
                call('POST', prefix + '/query', {'query': 'alpha'}, key=key, status=403)
            finally:
                pause.unlink(missing_ok=True)
            job.result(timeout=30)
        restart_with({})
        assert call('POST', prefix + '/doc', request)['receipt'] == receipt
        page = call('POST', prefix + '/query', {'query': 'alpha'})
        assert [hit['record']['record_id'] for hit in page['hits']] == ['concurrent'], page
        call('POST', prefix + '/query', {'query': 'alpha'}, key=key, status=403)
        indexed = call('PATCH', prefix)
        assert indexed['indexed_seq'] == indexed['visible_seq'] == 4, indexed
        print('PASS Indexer HTTP: concurrent acknowledged write, source deletion, credential revoke without cancellation, complete restart tail', flush=True)
    finally:
        pause.unlink(missing_ok=True)
        started.unlink(missing_ok=True)
