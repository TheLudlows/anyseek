"""Content-free feed, cursor continuity and failure acceptance on temporary data."""
import base64
import http.client
import os
import time
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
    for method,suffix,body in [('GET','/bootstrap',None),('POST','/bootstrap',{}),('DELETE','/bootstrap/'+'a'*32,None)]:
        assert call(method,endpoint+suffix,body,key=reader,status=404)['code']=='not_found'
    assert call('POST',endpoint+'/changes',{'after':'invalid'},key=reader,status=400)['code']=='rejected'
    ordinary={'operation_id':operation(),'record_id':'normal','routing_key':'r','expected':'Absent','document':{'body':'later body'}}
    call('POST',endpoint+'/doc',ordinary)
    tail=call('POST',endpoint+'/changes',{'after':event['next']},key=reader)
    assert len(tail['events'])==1 and tail['events'][0]['record_id']=='normal'
    assert call('POST',endpoint+'/doc',{**ordinary,'operation_id':operation(),'record_id':'full'},status=429)['code']=='feed_full'
    deletion={**source,'operation_id':operation(),'expected':{'Match':1},'document':None}
    deletion.pop('system')
    call('POST',endpoint+'/doc',deletion)
    notices=call('POST',endpoint+'/changes',{'after':tail['next']},key=reader)
    assert notices['events'][0]['rescan_required'] and notices['events'][0]['kind']=='source_deleted'
    call('POST',endpoint+'/grants',{'operation_id':operation(),'key_id':'feed-reader','allow':False})
    call('POST',endpoint+'/changes',{},key=reader,status=403)
    restart_with({})
    assert call('POST',endpoint+'/changes',{})['events']==event['events']+tail['events']+notices['events']
    print('PASS feed HTTP: shared authorization, idempotent notices, cursor continuation, capacity backpressure, reserved deletion, revoke, restart')
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
        # Missing history stays an explicit error even after another request.
        assert call('POST',target+'/changes',{},status=410)['code']=='cursor_expired'
        assert call('GET',target+'/doc/normal')['document']==ordinary['document']
    boundary='SQLite-confirmed durable pending/process exit' if os.environ.get('ANYSEEK_STORAGE_BACKEND')=='s3' else 'unknown/durable/process exit'
    print('PASS feed trim faults: seven-day boundary, '+boundary+' recovery, expired cursor, retained retry receipt and current record')
