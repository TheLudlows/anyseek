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
    call('POST',endpoint+'/grants',{'operation_id':operation(),'key_id':'temporal-reader','allow':True})
    source = {'operation_id': operation(), 'record_id': 's', 'routing_key': 'route', 'expected': 'Absent',
              'document': {'body': 'temporal source'}, 'system': {'kind': 'source'},
              'temporal': {'event_time_ms': 100, 'valid_from_ms': 100, 'valid_to_ms': 200, 'expire_at_ms': now + (100 if faults else 86400000)}}
    receipt = call('POST', endpoint + '/doc', source)['receipt']
    for rid, parent in [('d', 's'), ('i', 'd')]:
        call('POST', endpoint + '/doc', {'operation_id': operation(), 'record_id': rid, 'routing_key': 'route', 'expected': 'Absent',
             'document': {'body': 'temporal derived'}, 'system': {'kind': 'derived', 'evidence_refs': [{'record_id': parent, 'routing_key': 'route', 'version': 1}]}})
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
        clock(now + 100)
        for rid in ['s', 'd', 'i']:
            call('GET', endpoint + '/doc/' + rid, status=404)
            assert call('GET', endpoint + '/doc/' + rid + '/status')['visibility'] == 'expired'
        assert call('POST', endpoint + '/query', {})['statistics']['active_records'] == 0
        assert call('POST', endpoint + '/doc', source)['receipt'] == receipt
        call('POST', endpoint + '/doc', {**source, 'operation_id': operation(), 'expected': {'Match': 2}, 'temporal': {'expire_at_ms': now + 10000}}, status=400)
        # Final-state takeover of an old backup must retain the expiration cause.
        take = {'operation_id': operation(), 'source_collection_id': 'temporal', 'collection_id': 'temporal-next', 'backup_id': archive['backup_id']}
        assert call('POST', '/api/v1/takeovers', take)['status'] == 'active'
        endpoint = '/api/v1/index/temporal-next'
        call('POST',endpoint+'/query',{},key=reader,status=403)
        call('GET', '/api/v1/index/temporal', status=403)
        assert call('POST', endpoint + '/query', {})['count_total'] == 0
        restart_with(env)
        assert call('POST', endpoint + '/query', {})['count_total'] == 0
        clock(now - 300001)  # Larger backward jump is rejected; it cannot restore expired data.
        assert call('POST', endpoint + '/query', {}, status=503)['code'] == 'clock_uncertain'
        clock(now + 101)
        assert call('POST', endpoint + '/query', {})['count_total'] == 0
        print('PASS temporal HTTP: fixed view, transitive expiry, no revival, original retry, restart and final-state takeover')
        for stage in ['after_append_unknown', 'after_durable', 'crash_after_durable', 'crash_after_visible']:
            # All commands use the same controlled time; no sleep determines expiry.
            cid = 'ttl-' + stage.replace('_', '-')
            endpoint = '/api/v1/index/' + cid
            call('POST', '/api/v1/index', {'operation_id': operation(), 'collection_id': cid, 'config': config})
            write = {**source, 'operation_id': operation(), 'temporal': {'expire_at_ms': now + 200}}
            original = call('POST', endpoint + '/doc', write)['receipt']
            restart_with({**env, 'ANYSEEK_TEMPORAL_FAULT': stage})
            clock(now + 200)
            if stage.startswith('crash_'):
                try:
                    call('POST', endpoint + '/query', {})
                except (OSError, http.client.HTTPException):
                    pass
                else:
                    raise AssertionError('expected temporal process exit')
            else:
                code = call('POST', endpoint + '/query', {}, status=503)['code']
                assert code == ('outcome_unknown' if stage == 'after_append_unknown' else 'durable_pending'), (stage, code)
                call('GET', endpoint + '/doc/s', status=503)
            restart_with(env)
            assert call('POST', endpoint + '/query', {})['count_total'] == 0
            assert call('GET', endpoint + '/doc/s/status')['version'] == 2
            assert call('POST', endpoint + '/doc', write)['receipt'] == original
            # Keep the trusted clock monotonic across fixtures.
            now += 1000
            clock(now + 101)
        print('PASS temporal HTTP fault matrix: append unknown, durable pending, exit after durable/visible, terminal replay')
    else:
        print('PASS temporal HTTP: ranges, missing event, valid interval boundary, count-only, common time and restart')
    # Retire fixtures before returning to the real clock; a large test jump is
    # deliberately not treated as an acceptable restart clock rollback.
    if faults:
        for cid in ['temporal-next'] + ['ttl-' + stage.replace('_', '-') for stage in ['after_append_unknown', 'after_durable', 'crash_after_durable', 'crash_after_visible']]:
            call('DELETE', '/api/v1/index/' + cid, {'operation_id': operation()})
    call('DELETE','/api/v1/apikey',{'operation_id':operation(),'key_id':'temporal-reader'})
    call('DELETE', '/api/v1/backups/' + archive['backup_id'], {'operation_id': operation()})
    restart_with({})
