"""Real HTTP committed ingestion, original-identity recovery and lost responses."""
import json
import pathlib
import socket
import time
from urllib.parse import urlsplit
from anyseek_client import anyseekClient
from record_service_smoke import operation


def exercise(call, restart, directory, faults, base, master):
    path = '/api/v1/index/committed'
    root = pathlib.Path(directory) / 'data'
    existing = set(root.glob('collection-*'))
    call('POST', '/api/v1/index', {'operation_id': operation(), 'collection_id': 'committed',
         'config': {'index_name': 'committed', 'schema': [{'field': 'body', 'field_type': 'Text', 'store': True}]}})

    def write(identity):
        return {'operation_id': operation(), 'record_id': identity, 'routing_key': identity,
                'expected': 'Absent', 'document': {'body': 'alpha ' + identity}}

    client = anyseekClient(base)
    client.set_default_key(master)
    collection, = set(root.glob('collection-*')) - existing
    pauses = [collection / ('.test-' + lane + '-consumer-pause') for lane in ['lexical', 'vector']]
    if faults:
        for pause in pauses:
            pause.write_text('pause')
    first, conflict, later = write('r'), write('r'), write('s')
    call('POST', path + '/ingest', {'records': []}, status=400)
    result = client.ingest('committed', [first, conflict, later])
    assert [r['code'] for r in result['results']] == ['committed', 'rejected', 'committed'], result
    assert result['results'][1]['error']['code'] == 'conflict'
    progress = client.ingest_progress('committed')
    assert progress['committed_seq'] == 2, progress
    assert not any(k in progress for k in ['durable_seq', 'completed_seq', 'materialized_seq'])
    if faults:
        assert progress['lexical_seq'] == progress['vector_seq'] == 0, progress
        for pause in pauses:
            pause.unlink()
    assert not (collection / 'intake').exists()
    receipt = result['results'][0]['receipt']
    assert client.ingest_status('committed', first['operation_id'])['receipt'] == receipt
    restart({})
    replay = client.ingest('committed', [first, conflict, later])
    assert replay['results'][0]['receipt'] == receipt
    assert replay['results'][2]['receipt'] == result['results'][2]['receipt']
    print('PASS committed ingest HTTP: mixed results, later execution, Record progress, retry and restart', flush=True)
    if not faults:
        return

    # Authorization owns work even if the client drops the socket while waiting.
    pause = pathlib.Path(directory) / 'ingest-authorized-pause'
    pause.write_text('pause')
    restart({'ANYSEEK_AUTHORIZED_PAUSE_COLLECTION': 'committed', 'ANYSEEK_AUTHORIZED_PAUSE_FILE': str(pause)})
    lost = write('lost')
    payload = json.dumps({'records': [lost]}).encode()
    address = urlsplit(base)
    stream = socket.create_connection((address.hostname, address.port), timeout=5)
    stream.sendall((f'POST {path}/ingest HTTP/1.1\r\nHost: localhost\r\napikey: {master}\r\n'
                    f'Content-Type: application/json\r\nContent-Length: {len(payload)}\r\nConnection: close\r\n\r\n').encode() + payload)
    deadline = time.monotonic() + 10
    while not pause.with_suffix('.started').exists():
        assert time.monotonic() < deadline, 'authorized request did not start'
        time.sleep(.01)
    stream.close()
    pause.unlink()
    while True:
        status = client.ingest_status('committed', lost['operation_id'])
        if status['receipt'] is not None:
            break
        assert time.monotonic() < deadline, 'cancelled request did not commit'
        time.sleep(.02)
    restart({})
    assert client.ingest('committed', [lost])['results'][0]['receipt'] == status['receipt']

    for fault in ['busy_before_append', 'after_append_unknown', 'after_durable']:
        new, conflict_new, suffix = write(fault), write(fault), write(fault + '-tail')
        commands = [first, conflict, new, conflict_new, suffix]
        restart({'ANYSEEK_RECORD_FAULT': fault, 'ANYSEEK_RECORD_FAULT_NONCE': new['operation_id']['nonce']})
        code = 429 if fault == 'busy_before_append' else 503
        result = call('POST', path + '/ingest', {'records': commands}, status=code)
        expected = ['committed', 'rejected', 'not_attempted' if code == 429 else 'outcome_unknown', 'not_attempted', 'not_attempted']
        assert [r['code'] for r in result['results']] == expected, result
        if code == 503:
            progress = client.ingest_progress('committed')
            assert progress['recovery_required'] and progress['committed_seq'] is None, progress
        # The client must return the entire 429/503 envelope, not discard receipts.
        again = client.ingest('committed', commands)
        assert len(again['results']) == len(commands), again
        restart({})
        retried = client.ingest('committed', commands)
        assert [r['code'] for r in retried['results']] == ['committed', 'rejected', 'committed', 'rejected', 'committed'], retried
        assert retried['results'][0]['receipt'] == receipt
        assert client.ingest_status('committed', new['operation_id'])['receipt'] == retried['results'][2]['receipt']
    print('PASS committed ingest HTTP faults: paused indexes, disconnect, 429/503 item scopes, original-id restart recovery', flush=True)
