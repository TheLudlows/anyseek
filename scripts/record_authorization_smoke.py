"""Service credential checks, namespace isolation and owned HTTP work."""
import base64
import concurrent.futures
import os
import time
from record_service_smoke import operation
from anyseek_client import write_one


def exercise(call, restart_with, work, faults):
    assert faults, 'authorization pause needs the explicit fault-injection binary'
    pause = work / 'authorization.pause'
    started = pause.with_suffix('.started')
    invalid = base64.b64encode(os.urandom(32)).decode()
    try:
        restart_with({'ANYSEEK_AUTHORIZED_PAUSE_COLLECTION': 'slow',
                      'ANYSEEK_AUTHORIZED_PAUSE_FILE': str(pause)})
        controls = {}
        for collection in ['slow', 'other']:
            controls[collection] = {'operation_id': operation(), 'collection_id': collection,
                'config': {'index_name': collection, 'schema': [{'field': 'body', 'field_type': 'Text'}]}}
            call('POST', '/api/v1/index', controls[collection])
        request = {'operation_id': operation(), 'record_id': 'accepted',
                   'expected': 'Absent', 'document': {'body': 'alpha accepted'}}
        pause.write_text('pause accepted operation')
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            first = pool.submit(write_one, call, '/api/v1/index/slow', request)
            try:
                deadline = time.monotonic() + 10
                while not started.exists():
                    assert not first.done(), 'work returned before the authorized pause'
                    assert time.monotonic() < deadline, 'authorized pause was not reached'
                    time.sleep(.01)
                other = {'operation_id': operation(), 'record_id': 'parallel',
                         'expected': 'Absent', 'document': {'body': 'beta independent'}}
                receipt = pool.submit(write_one, call, '/api/v1/index/other', other).result(timeout=5)['receipt']
                page = pool.submit(call, 'POST', '/api/v1/index/other/query', {'query': 'beta'}).result(timeout=5)
                assert page['hits'][0]['record']['record_id'] == 'parallel', page
                identity = controls['other']['operation_id']
                route = f"/api/v1/catalog/operation/{identity['epoch']}/{identity['nonce']}"
                assert pool.submit(call, 'GET', route).result(timeout=5)['receipt'] is not None
                assert pool.submit(call, 'GET', route, tenant='foreign').result(timeout=5)['receipt'] is None
                pool.submit(call, 'GET', '/api/v1/index/other', key=invalid, status=401).result(timeout=5)
                pool.submit(call, 'GET', '/api/v1/index/other', tenant='foreign', status=403).result(timeout=5)
            finally:
                pause.unlink(missing_ok=True)
            first.result(timeout=30)
        assert call('GET', '/api/v1/index/slow/doc/accepted')['version'] == 1
        accepted = write_one(call, '/api/v1/index/slow', request)['receipt']
        restart_with({})
        assert write_one(call, '/api/v1/index/slow', request)['receipt'] == accepted
        assert write_one(call, '/api/v1/index/other', other)['receipt'] == receipt
        call('GET', '/api/v1/index/slow/doc/accepted', key=invalid, status=401)
        call('GET', '/api/v1/index/slow/doc/accepted', tenant='foreign', status=403)
        print('PASS gateway HTTP: service credential checks, namespace and receipt isolation, unrelated progress and owned work after restart', flush=True)
    finally:
        pause.unlink(missing_ok=True)
        started.unlink(missing_ok=True)
