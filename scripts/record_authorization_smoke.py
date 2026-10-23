"""Accepted work, scoped revocation and unrelated-collection HTTP progress."""
import base64
import concurrent.futures
import os
import time
from record_service_smoke import operation


def exercise(call, restart_with, work, faults):
    assert faults, 'authorization pause needs the explicit fault-injection binary'
    pause = work / 'authorization.pause'
    started = pause.with_suffix('.started')
    key = base64.b64encode(os.urandom(32)).decode()
    try:
        restart_with({'ANYSEEK_AUTHORIZED_PAUSE_COLLECTION': 'slow',
                      'ANYSEEK_AUTHORIZED_PAUSE_FILE': str(pause)})
        call('POST', '/api/v1/apikey', {'operation_id': operation(), 'key_id': 'scoped-key', 'apikey_base64': key})
        for collection in ['slow', 'other']:
            call('POST', '/api/v1/index', {'operation_id': operation(), 'collection_id': collection,
                 'config': {'index_name': collection, 'schema': [{'field': 'body', 'field_type': 'Text'}]}})
        call('POST', '/api/v1/index/slow/grants', {'operation_id': operation(), 'key_id': 'scoped-key', 'allow': True})
        request = {'operation_id': operation(), 'record_id': 'accepted', 'routing_key': 'r',
                   'expected': 'Absent', 'document': {'body': 'alpha accepted'}}
        pause.write_text('pause accepted operation')
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            first = pool.submit(call, 'POST', '/api/v1/index/slow/doc', request, key=key, status=(200, 403))
            try:
                deadline = time.monotonic() + 10
                while not started.exists():
                    assert not first.done(), 'work returned before the authorized pause'
                    assert time.monotonic() < deadline, 'authorized pause was not reached'
                    time.sleep(.01)
                # Each assertion needs to finish while the first write is paused.
                other = {'operation_id': operation(), 'record_id': 'parallel', 'routing_key': 'r',
                         'expected': 'Absent', 'document': {'body': 'beta independent'}}
                receipt = pool.submit(call, 'POST', '/api/v1/index/other/doc', other).result(timeout=5)['receipt']
                page = pool.submit(call, 'POST', '/api/v1/index/other/query', {'query': 'beta'}).result(timeout=5)
                assert page['hits'][0]['record']['record_id'] == 'parallel', page
                revoke = pool.submit(call, 'POST', '/api/v1/index/slow/grants',
                                     {'operation_id': operation(), 'key_id': 'scoped-key', 'allow': False})
                # A pending scoped revoke must also leave other collections free.
                assert pool.submit(call, 'GET', '/api/v1/index/other/doc/parallel').result(timeout=5)['version'] == 1
            finally:
                pause.unlink(missing_ok=True)
            first.result(timeout=30)
            revoke.result(timeout=30)
        call('POST', '/api/v1/index/slow/doc', {**request, 'operation_id': operation(), 'record_id': 'denied'}, key=key, status=403)
        assert call('GET', '/api/v1/index/slow/doc/accepted')['version'] == 1
        accepted = call('POST', '/api/v1/index/slow/doc', request)['receipt']
        restart_with({})
        assert call('POST', '/api/v1/index/slow/doc', request)['receipt'] == accepted
        assert call('POST', '/api/v1/index/other/doc', other)['receipt'] == receipt
        call('GET', '/api/v1/index/slow/doc/accepted', key=key, status=403)
        print('PASS authorization HTTP: unrelated collection progress, scoped revoke, accepted write and receipts survive restart', flush=True)
    finally:
        pause.unlink(missing_ok=True)
        started.unlink(missing_ok=True)
