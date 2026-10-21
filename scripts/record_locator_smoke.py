"""Exact inline source locations; temporary data only."""
import base64
import hashlib
import http.client
import os
from record_service_smoke import operation


def exercise(call,restart_with,faults):
    endpoint='/api/v1/index/locators'
    call('POST','/api/v1/index',{'operation_id':operation(),'collection_id':'locators','config':{'index_name':'Locators','schema':[{'field':'body','field_type':'Text','store':True}]}})
    source={'operation_id':operation(),'record_id':'s','routing_key':'r','expected':'Absent','document':{'body':'A中éZ','pages':['第一页','page two']},'system':{'kind':'source'}}
    call('POST',endpoint+'/doc',source)
    ref={'record_id':'s','routing_key':'r','version':1,'locator':{'kind':'utf8','field':'body','start':1,'end':6}}
    excerpt=call('POST',endpoint+'/evidence',ref)
    assert excerpt['text']=='中é' and excerpt['content_sha256']==hashlib.sha256('A中éZ'.encode()).hexdigest()
    assert call('POST',endpoint+'/evidence',{**ref,'locator':{'kind':'page','field':'pages','page':2}})['text']=='page two'
    call('POST',endpoint+'/evidence',{**ref,'locator':{'kind':'page','field':'pages','page':0}},status=400)
    def derived(rid,reference=ref):
        return {'operation_id':operation(),'record_id':rid,'routing_key':'r','expected':'Absent','document':{'body':'located proof'},'system':{'kind':'derived','evidence_refs':[reference]}}
    bad={**ref,'locator':{**ref['locator'],'start':2}}
    call('POST',endpoint+'/doc',derived('bad',bad),status=400)
    request=derived('d');receipt=call('POST',endpoint+'/doc',request)['receipt']
    assert call('POST',endpoint+'/doc',request)['receipt']==receipt
    if faults:
        for stage in ['after_append_unknown','after_durable','crash_after_durable','crash_after_visible']:
            request=derived('d-'+stage)
            restart_with({'SEEKPOWER_RECORD_FAULT':stage,'SEEKPOWER_RECORD_FAULT_NONCE':request['operation_id']['nonce']})
            if stage.startswith('crash_'):
                try:call('POST',endpoint+'/doc',request)
                except (OSError,http.client.HTTPException):pass
                else:raise AssertionError('expected located evidence process exit')
            else:assert call('POST',endpoint+'/doc',request,status=503)['code'] in ('outcome_unknown','durable_pending')
            restart_with({});receipt=call('POST',endpoint+'/doc',request)['receipt']
            assert call('POST',endpoint+'/doc',request)['receipt']==receipt
            assert call('POST',endpoint+'/evidence',ref)['text']=='中é'
    call('POST',endpoint+'/doc',{**source,'operation_id':operation(),'expected':{'Match':1},'document':{'body':'new'}})
    call('GET',endpoint+'/doc/d',status=404)
    assert call('POST',endpoint+'/evidence',ref)['content_sha256']==excerpt['content_sha256']
    call('PATCH',endpoint,{})
    restart_with({});assert call('POST',endpoint+'/evidence',ref)['text']=='中é'
    reader=base64.b64encode(os.urandom(32)).decode()
    call('POST','/api/v1/apikey',{'operation_id':operation(),'key_id':'locator-reader','apikey_base64':reader})
    call('POST',endpoint+'/grants',{'operation_id':operation(),'key_id':'locator-reader','allow':True})
    assert call('POST',endpoint+'/evidence',ref,key=reader)['text']=='中é'
    call('POST',endpoint+'/grants',{'operation_id':operation(),'key_id':'locator-reader','allow':False})
    call('POST',endpoint+'/evidence',ref,key=reader,status=403)
    archive=call('POST',endpoint+'/backup',{'operation_id':operation()})
    call('POST',endpoint+'/doc',{'operation_id':operation(),'record_id':'s','routing_key':'r','expected':{'Match':2},'document':None})
    call('POST',endpoint+'/evidence',ref,status=409)
    call('POST','/api/v1/restore',{'operation_id':operation(),'collection_id':'locators-copy','backup_id':archive['backup_id']})
    call('POST','/api/v1/index/locators-copy/evidence',ref,status=409)
    assert call('POST','/api/v1/takeovers',{'operation_id':operation(),'source_collection_id':'locators','collection_id':'locators-next','backup_id':archive['backup_id']})['status']=='active'
    restart_with({});call('POST','/api/v1/index/locators-next/evidence',ref,status=409)
    call('DELETE','/api/v1/backups/'+archive['backup_id'],{'operation_id':operation()})
    call('DELETE','/api/v1/apikey',{'operation_id':operation(),'key_id':'locator-reader'})
    print('PASS locator HTTP: UTF-8/page bounds, exact history/hash, stale/delete/revoke, checkpoint, quarantine and takeover'+(' with process faults' if faults else ''))
