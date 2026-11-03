"""TTL/time acceptance invoked by the existing native service matrix.
Only temporary fixture collections and the feature-gated file clock are used.
"""
import http.client
import base64
import json
import os
import time
import urllib.error
from record_service_smoke import operation
from anyseek_client import write_one


def exercise(call, restart_with, directory, faults):
    now = int(time.time() * 1000)
    clock_file = directory / 'clock-ms'

    def clock(value):
        pending = clock_file.with_suffix('.next')
        pending.write_text(str(value))
        os.replace(pending, clock_file)

    clock(now)
    env = {'ANYSEEK_TEST_CLOCK_FILE': str(clock_file)} if faults else {}
    restart_with(env)
    config = {'index_name': 'Temporal records', 'schema': [{'field': 'body', 'field_type': 'Text', 'store': True}]}
    call('POST', '/api/v1/index', {'operation_id': operation(), 'collection_id': 'temporal', 'config': config})
    endpoint = '/api/v1/index/temporal'
    reader=base64.b64encode(os.urandom(32)).decode()
    call('POST','/api/v1/apikey',{'operation_id':operation(),'key_id':'temporal-reader','apikey_base64':reader})
    source = {'operation_id': operation(), 'record_id': 's',  'expected': 'Absent',
              'document': {'body': 'temporal source'},
              'temporal': {'event_time_ms': 100, 'valid_from_ms': 100, 'valid_to_ms': 200, 'expire_at_ms': now + (100 if faults else 86400000)}}
    receipt = write_one(call, endpoint, source)['receipt']
    for rid, _parent in [('d', 's'), ('i', 'd')]:
        write_one(call, endpoint, {'operation_id': operation(), 'record_id': rid,  'expected': 'Absent',
             'document': {'body': 'temporal peer'}, 'temporal': {'expire_at_ms': source['temporal']['expire_at_ms']}})
    query = {'query': 'temporal', 'temporal': {'event': {'from_ms': 100, 'to_ms': 101}, 'valid_at_ms': 100}}
    result = call('POST', endpoint + '/query', query)
    assert [h['record']['record_id'] for h in result['hits']] == ['s'], result
    assert call('POST',endpoint+'/query',query,key=reader)['count_total']==1
    assert result['evaluation_time_ms'] == result['statistics']['evaluation_time_ms']
    assert call('POST', endpoint + '/query', {**query, 'temporal': {'valid_at_ms': 200}})['count_total'] == 2
    assert call('POST', endpoint + '/query', {**query, 'length': 0})['count_total'] == 1
    call('POST', endpoint + '/query', {'temporal': {'event': {'from_ms': 100, 'to_ms': 100}}}, status=400)
    archive = call('POST', endpoint + '/backup', {'operation_id': operation()})
    restart_with(env)
    assert call('POST', endpoint + '/query', query)['count_total'] == 1
    if faults:
        def reclaimed(target, version):
            deadline=time.monotonic()+30
            while time.monotonic()<deadline:
                result=call('GET',target+'/doc/s/status',status=(200,503))
                if result.get('version',0)>=version:return result
                if 'code' in result:assert result['code']=='recovery_required',result
                time.sleep(.05)
            raise AssertionError(('background TTL reclaim deadline',target,result))

        clock(now + 100)
        for rid in ['s', 'd', 'i']:
            call('GET', endpoint + '/doc/' + rid, status=404)
        assert call('POST', endpoint + '/query', {})['statistics']['active_records'] == 0
        assert write_one(call, endpoint, source)['receipt'] == receipt
        # Wait for background persistence before checking the persisted floor.
        # Reads themselves only filter; the version changes only after GC.
        reclaimed(endpoint,2)
        restart_with(env)
        assert call('POST', endpoint + '/query', {})['count_total'] == 0
        clock(now - 300001)
        assert call('POST', endpoint + '/query', {}, status=503)['code'] == 'clock_uncertain'
        clock(now + 101)
        assert call('POST', endpoint + '/query', {})['count_total'] == 0
        reuse={**source,'operation_id':operation(),'expected':'Absent','temporal':None}
        assert write_one(call,endpoint,reuse)['receipt']['version']==3
        assert call('GET',endpoint+'/doc/s')['document']==source['document']
        print('PASS temporal HTTP: logical filtering, background reclaim, durable floor, ID reuse and original retry')
        for stage in ['after_append_unknown', 'after_durable', 'crash_after_durable', 'crash_after_visible']:
            # All commands use the same controlled time; no sleep determines expiry.
            cid = 'ttl-' + stage.replace('_', '-')
            endpoint = '/api/v1/index/' + cid
            call('POST', '/api/v1/index', {'operation_id': operation(), 'collection_id': cid, 'config': config})
            write = {**source, 'operation_id': operation(), 'temporal': {'expire_at_ms': now + 200}}
            original = write_one(call, endpoint, write)['receipt']
            restart_with({**env, 'ANYSEEK_TEMPORAL_FAULT': stage})
            # Reopen loads partitions lazily. Capture it while still live, then
            # move the clock; the service sweep, not a query, attempts expiry.
            call('GET',endpoint+'/doc/s')
            clock(now + 200)
            deadline=time.monotonic()+30
            observed=False
            while time.monotonic()<deadline:
                try:
                    result=call('GET',endpoint+'/doc/s',status=(404,503))
                except (OSError,http.client.HTTPException):
                    if not stage.startswith('crash_'):raise
                    observed=True;break
                if 'code' in result and result['code']=='recovery_required':
                    # Background owns persistence. Fresh reads report the fence,
                    # not the worker's unknown/durable_pending result envelope.
                    if not stage.startswith('crash_'):observed=True;break
                else:assert result.get('code')=='not_found',result
                time.sleep(.05)
            assert observed,('background temporal fault deadline',stage)
            restart_with(env)
            assert call('POST', endpoint + '/query', {})['count_total'] == 0
            call('GET', endpoint + '/doc/s', status=404)
            assert write_one(call, endpoint, write)['receipt'] == original
            # Keep the trusted clock monotonic across fixtures.
            now += 1000
            clock(now + 101)
        print('PASS temporal HTTP fault matrix: append unknown, durable pending, exit after durable/visible, background fence and replay')
    else:
        print('PASS temporal HTTP: ranges, missing event, valid interval boundary, count-only, common time and restart')
    # Retire fixtures before returning to the real clock; a large test jump is
    # deliberately not treated as an acceptable restart clock rollback.
    if faults:
        for cid in ['temporal'] + ['ttl-' + stage.replace('_', '-') for stage in ['after_append_unknown', 'after_durable', 'crash_after_durable', 'crash_after_visible']]:
            call('DELETE', '/api/v1/index/' + cid, {'operation_id': operation()})
    call('DELETE','/api/v1/apikey',{'operation_id':operation(),'key_id':'temporal-reader'})
    call('DELETE', '/api/v1/backups/' + archive['backup_id'], {'operation_id': operation()})
    restart_with({})
