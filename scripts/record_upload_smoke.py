"""Multipart generation over real HTTP, temporary collections only."""
import http.client
from record_service_smoke import operation


def exercise(call,restart_with,faults):
    config={'index_name':'Multipart','schema':[{'field':'body','field_type':'Text','store':True}]}
    def create(cid):
        endpoint='/api/v1/index/'+cid
        call('POST','/api/v1/index',{'operation_id':operation(),'collection_id':cid,'config':config})
        call('POST',endpoint+'/doc',{'operation_id':operation(),'record_id':'s','expected':'Absent','document':{'body':'source'},'system':{'kind':'source'}})
        return endpoint
    def command(action,version,**kwargs):
        return {'operation_id':operation(),'record_id':'doc','expected':'Absent' if version==0 else {'Match':version},'generation':{'action':action,'generation_id':'g',**kwargs}}
    def begin(members):return command('begin',0,source={'record_id':'s','version':1},members=members)
    endpoint=create('multipart');call('POST',endpoint+'/doc',begin(['one','two']))
    first=command('stage_batch',1,chunks=[{'record_id':'two','document':{'body':'multipartword','payload':'x'*600000}}])
    receipt=call('POST',endpoint+'/doc',first)['receipt'];assert receipt['staged_only']
    call('GET',endpoint+'/doc/two',status=404)
    status=call('GET',endpoint+'/doc/doc/generation');assert status['expected_members']==2 and status['staged_members']==1 and not status['sealed']
    call('POST',endpoint+'/doc',command('publish',2),status=409)
    call('POST',endpoint+'/doc',command('seal',2,manifest_sha256=status['manifest_sha256']),status=400)
    archive=call('POST',endpoint+'/backup',{'operation_id':operation()})
    second=command('stage_batch',2,chunks=[{'record_id':'one','document':{'body':'multipartword','payload':'y'*600000}}])
    # A backup leaves the live owner's scope and lease intact; continue uploading.
    assert not call('GET',endpoint+'/doc/doc/generation')['lease_requires_renewal']
    second_receipt=call('POST',endpoint+'/doc',second)['receipt']
    assert second_receipt['version']==3 and second_receipt['staged_only']
    assert call('POST',endpoint+'/doc',second)['receipt']==second_receipt
    assert call('POST',endpoint+'/doc',first)['receipt']==receipt
    status=call('GET',endpoint+'/doc/doc/generation');assert status['staged_members']==2
    call('POST',endpoint+'/doc',command('seal',3,manifest_sha256='0'*64),status=409)
    seal=command('seal',3,manifest_sha256=status['manifest_sha256']);call('POST',endpoint+'/doc',seal)
    restart_with({});assert call('GET',endpoint+'/doc/doc/generation')['sealed']
    assert call('POST',endpoint+'/query',{'query':'multipartword'})['count']==0
    publish=command('publish',4);published=call('POST',endpoint+'/doc',publish)['receipt'];assert not published['staged_only']
    assert call('POST',endpoint+'/query',{'query':'multipartword'})['count']==2
    call('PATCH',endpoint,{})
    restart_with({});assert call('POST',endpoint+'/doc',publish)['receipt']==published
    assert call('POST',endpoint+'/query',{'query':'multipartword'})['count']==2
    call('DELETE','/api/v1/backups/'+archive['backup_id'],{'operation_id':operation()})
    if faults:
        for target in ['begin','stage_batch','seal','publish']:
            for fault in ['after_append_unknown','after_durable','crash_after_durable','crash_after_visible']:
                endpoint=create('upload-'+target+'-'+fault)
                for index,action in enumerate(['begin','stage_batch','seal','publish']):
                    if action=='begin':request=begin(['chunk'])
                    elif action=='stage_batch':request=command(action,1,chunks=[{'record_id':'chunk','document':{'body':'faultupload'}}])
                    elif action=='seal':request=command(action,2,manifest_sha256=call('GET',endpoint+'/doc/doc/generation')['manifest_sha256'])
                    else:request=command(action,3)
                    if action==target:
                        restart_with({'ANYSEEK_RECORD_FAULT':fault,'ANYSEEK_RECORD_FAULT_NONCE':request['operation_id']['nonce']})
                        if fault.startswith('crash_'):
                            try:call('POST',endpoint+'/doc',request)
                            except (OSError,http.client.HTTPException):pass
                            else:raise AssertionError('expected upload process exit')
                        else:assert call('POST',endpoint+'/doc',request,status=503)['code'] in ('outcome_unknown','durable_pending')
                        restart_with({})
                    receipt=call('POST',endpoint+'/doc',request)['receipt']
                    assert call('POST',endpoint+'/doc',request)['receipt']==receipt
                    assert call('POST',endpoint+'/query',{'query':'faultupload'})['count']==(1 if action=='publish' else 0)
    print('PASS multipart HTTP: >1 MiB across batches, incomplete/seal fences, retained retry, same-owner continuation and checkpoint'+(' and all four commands x four process fault points' if faults else ''))
