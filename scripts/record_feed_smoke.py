"""Content-free feed, snapshot cut, scope and failure acceptance on temporary data."""
import base64
import http.client
import os
import time
import uuid
from record_service_smoke import operation


def exercise(call, restart_with, directory, faults):
    config={'index_name':'Feed records','schema':[{'field':'body','field_type':'Text','store':True}],
            'change_feed':{'max_events':3,'max_bytes':65536,'retention_ms':7*86400000}}
    call('POST','/api/v1/index',{'operation_id':operation(),'collection_id':'feed','config':config})
    endpoint='/api/v1/index/feed'
    reader=base64.b64encode(os.urandom(32)).decode()
    call('POST','/api/v1/apikey',{'operation_id':operation(),'key_id':'feed-reader','apikey_base64':reader})
    call('POST',endpoint+'/grants',{'operation_id':operation(),'key_id':'feed-reader','allow':True})
    source={'operation_id':operation(),'record_id':'source','routing_key':'r','expected':'Absent','document':{'body':'private body'},'system':{'kind':'source'}}
    receipt=call('POST',endpoint+'/doc',source)['receipt']
    assert call('POST',endpoint+'/doc',source)['receipt']==receipt
    event=call('POST',endpoint+'/changes',{},key=reader)
    assert len(event['events'])==1 and event['events'][0]['kind']=='source_created'
    assert 'private body' not in str(event)
    identity=call('GET',endpoint+'/bootstrap',key=reader)['request_id_prefix']+uuid.uuid4().hex[:16]
    request={'request_id':identity,'length':1}
    snapshot=call('POST',endpoint+'/bootstrap',request,key=reader)
    assert snapshot['visible_seq']==receipt['seq'] and snapshot['records'][0]['record_id']=='source'
    assert call('POST',endpoint+'/bootstrap',request,key=reader)==snapshot
    ordinary={'operation_id':operation(),'record_id':'normal','routing_key':'r','expected':'Absent','document':{'body':'later body'}}
    call('POST',endpoint+'/doc',ordinary)
    assert call('POST',endpoint+'/bootstrap',request,key=reader)==snapshot
    tail=call('POST',endpoint+'/changes',{'after':snapshot['continuation']},key=reader)
    assert len(tail['events'])==1 and tail['events'][0]['record_id']=='normal'
    assert call('POST',endpoint+'/doc',{**ordinary,'operation_id':operation(),'record_id':'full'},status=429)['code']=='feed_full'
    deletion={**source,'operation_id':operation(),'expected':{'Match':1},'document':None}
    deletion.pop('system')
    call('POST',endpoint+'/doc',deletion)
    assert call('POST',endpoint+'/bootstrap',request,key=reader,status=410)['code']=='bootstrap_expired'
    notices=call('POST',endpoint+'/changes',{'after':tail['next']},key=reader)
    assert notices['events'][0]['rescan_required'] and notices['events'][0]['kind']=='source_deleted'
    call('POST',endpoint+'/grants',{'operation_id':operation(),'key_id':'feed-reader','allow':False})
    call('POST',endpoint+'/changes',{},key=reader,status=403)
    call('GET',endpoint+'/bootstrap',key=reader,status=403)
    restart_with({})
    assert call('POST',endpoint+'/changes',{})['events']==event['events']+tail['events']+notices['events']
    assert call('POST',endpoint+'/bootstrap',request,status=410)['code']=='bootstrap_expired'
    print('PASS feed HTTP: shared authorization, idempotent notices, stable snapshot cut/tail, capacity backpressure, reserved deletion, revoke, restart')
    if not faults:
        return
    now=int(time.time()*1000)
    clock_file=directory/'feed-clock-ms'
    def clock(value):
        pending=clock_file.with_suffix('.next');pending.write_text(str(value));os.replace(pending,clock_file)
    for index,stage in enumerate(['after_append_unknown','after_durable','crash_after_durable']):
        now+=8*86400000
        clock(now)
        env={'ANYSEEK_TEST_CLOCK_FILE':str(clock_file)}
        restart_with(env)
        cid='feed-trim-'+str(index);target='/api/v1/index/'+cid
        call('POST','/api/v1/index',{'operation_id':operation(),'collection_id':cid,'config':config})
        request={**ordinary,'operation_id':operation()}
        receipt=call('POST',target+'/doc',request)['receipt']
        page=call('POST',target+'/changes',{})
        clock(now+7*86400000-1)
        assert len(call('POST',target+'/changes',{})['events'])==1
        clock(now+7*86400000)
        restart_with({**env,'ANYSEEK_FEED_TRIM_FAULT':stage})
        if stage.startswith('crash_'):
            try:call('POST',target+'/changes',{})
            except (OSError,http.client.HTTPException):pass
            else:raise AssertionError('expected trim process exit')
        else:
            code=call('POST',target+'/changes',{},status=503)['code']
            if os.environ.get('ANYSEEK_STORAGE_BACKEND')=='s3':assert code=='durable_pending'
            else:assert code in ('outcome_unknown','durable_pending')
        restart_with(env)
        assert call('POST',target+'/changes',{},status=410)['code']=='cursor_expired'
        assert call('POST',target+'/changes',{'after':page['next']})['events']==[]
        assert call('POST',target+'/doc',request)['receipt']==receipt
        prefix=call('GET',target+'/bootstrap')['request_id_prefix']
        rebuilt=call('POST',target+'/bootstrap',{'request_id':prefix+uuid.uuid4().hex[:16]})
        assert rebuilt['visible_seq']==receipt['seq'] and len(rebuilt['records'])==1
    boundary='SQLite-confirmed durable pending/process exit' if os.environ.get('ANYSEEK_STORAGE_BACKEND')=='s3' else 'unknown/durable/process exit'
    print('PASS feed trim faults: seven-day boundary, '+boundary+' recovery, expired cursor, retained retry receipt and bootstrap')
