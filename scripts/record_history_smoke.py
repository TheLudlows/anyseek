"""Removed external sync routes and retained internal history/retry recovery."""
import http.client
import os
import time
from record_service_smoke import operation


def exercise(call, restart_with, directory, faults):
    config={'index_name':'History records','schema':[{'field':'body','field_type':'Text','store':True}]}
    call('POST','/api/v1/index',{'operation_id':operation(),'collection_id':'history','config':config})
    endpoint='/api/v1/index/history'
    for method,suffix,body in [('GET','/bootstrap',None),('POST','/bootstrap',{}),
                              ('DELETE','/bootstrap/'+'a'*32,None),('POST','/changes',{})]:
        assert call(method,endpoint+suffix,body,status=404)['code']=='not_found'
    # Removed event quotas must not remain accepted HTTP configuration knobs.
    call('POST','/api/v1/index',{'operation_id':operation(),'collection_id':'old-feed',
         'config':{**config,'change_feed':{'max_events':1}}},status=400)
    ordinary={'operation_id':operation(),'record_id':'normal','routing_key':'r',
              'expected':'Absent','document':{'body':'retained body'}}
    receipt=call('POST',endpoint+'/doc',ordinary)['receipt']
    restart_with({})
    assert call('POST',endpoint+'/doc',ordinary)['receipt']==receipt
    assert call('GET',endpoint+'/doc/normal')['document']==ordinary['document']
    assert len(call('POST',endpoint+'/iterator',{})['records'])==1
    print('PASS removed sync routes: 404, rejected legacy HTTP config, get/iterator and receipt retry after restart')
    if not faults:
        return
    now=int(time.time()*1000)
    clock_file=directory/'history-clock-ms'
    def clock(value):
        pending=clock_file.with_suffix('.next');pending.write_text(str(value));os.replace(pending,clock_file)
    for index,stage in enumerate(['after_append_unknown','after_durable','crash_after_durable']):
        now+=8*86400000
        clock(now)
        env={'ANYSEEK_TEST_CLOCK_FILE':str(clock_file)}
        restart_with(env)
        cid='history-trim-'+str(index);target='/api/v1/index/'+cid
        call('POST','/api/v1/index',{'operation_id':operation(),'collection_id':cid,'config':config})
        request={**ordinary,'operation_id':operation()}
        receipt=call('POST',target+'/doc',request)['receipt']
        clock(now+7*86400000-1)
        assert call('GET',target+'/doc/normal')['document']==ordinary['document']
        clock(now+7*86400000)
        restart_with({**env,'ANYSEEK_HISTORY_TRIM_FAULT':stage})
        if stage.startswith('crash_'):
            try:call('GET',target+'/doc/normal')
            except (OSError,http.client.HTTPException):pass
            else:raise AssertionError('expected trim process exit')
        else:
            code=call('GET',target+'/doc/normal',status=503)['code']
            if os.environ.get('ANYSEEK_STORAGE_BACKEND')=='s3':assert code=='durable_pending'
            else:assert code in ('outcome_unknown','durable_pending')
        restart_with(env)
        assert call('POST',target+'/doc',request)['receipt']==receipt
        assert call('GET',target+'/doc/normal')['document']==ordinary['document']
    boundary='SQLite-confirmed durable pending/process exit' if os.environ.get('ANYSEEK_STORAGE_BACKEND')=='s3' else 'unknown/durable/process exit'
    print('PASS history trim faults: seven-day boundary, '+boundary+' recovery, retained retry receipt and current record')
