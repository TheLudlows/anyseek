"""Real explicit-relation requests against temporary service collections."""
import base64
import os
import http.client
from record_service_smoke import operation


def exercise(call,restart_with,faults):
    endpoint='/api/v1/index/relations'
    config={'index_name':'Relations','schema':[{'field':'body','field_type':'Text','store':True}]}
    call('POST','/api/v1/index',{'operation_id':operation(),'collection_id':'relations','config':config})
    reader=base64.b64encode(os.urandom(32)).decode()
    call('POST','/api/v1/apikey',{'operation_id':operation(),'key_id':'relation-reader','apikey_base64':reader})
    call('POST',endpoint+'/grants',{'operation_id':operation(),'key_id':'relation-reader','allow':True})
    def write(rid,system,body='relation body'):
        return {'operation_id':operation(),'record_id':rid,'routing_key':'r','expected':'Absent','document':{'body':body},'system':system}
    def ref(rid):
        record=call('GET',endpoint+'/doc/'+rid)
        return {'record_id':rid,'routing_key':record['routing_key'],'version':record['version']}
    def edge(rid,left,right):
        return write(rid,{'kind':'relation','relation':{'from':ref(left),'to':ref(right),'relation_type':'knows','weight':.5}})
    def query(rid,**kwargs):
        return {'seeds':[ref(rid)],'per_seed':128,'max_candidates':1000,**kwargs}
    def fused(**kwargs):
        request={'query':'relation','length':20,'relation_fusion':{'neighbors':query('a'), 'rrf':{'constant':3,'keyword_weight':1,'vector_weight':0,'keyword_candidates':100},'relation_weight':2}}
        request.update(kwargs)
        return request
    a=write('a',{'kind':'entity'});b=write('b',{'kind':'entity'})
    call('POST',endpoint+'/doc',a);call('POST',endpoint+'/doc',b)
    ab=edge('ab','a','b');receipt=call('POST',endpoint+'/doc',ab)['receipt']
    assert call('POST',endpoint+'/doc',ab)['receipt']==receipt
    call('POST',endpoint+'/doc',edge('ba','b','a'))
    assert len(call('POST',endpoint+'/entities',{},key=reader)['records'])==2
    result=call('POST',endpoint+'/neighbors',query('a'),key=reader)
    assert result['complete'] and result['count']=={'value':2,'relation':'exact'}
    assert result['evaluation_time_ms']==result['statistics']['evaluation_time_ms']
    fusion=call('POST',endpoint+'/query',fused(),key=reader)
    assert fusion['fusion']['count_relation']=='candidate_window' and fusion['fusion']['relation_complete']
    assert fusion['visible_seq']==fusion['statistics']['visible_seq'] and fusion['evaluation_time_ms']==fusion['statistics']['evaluation_time_ms']
    assert fusion['hits'][0]['record']['record_id']=='b'
    for hit in fusion['hits']:
        assert abs(sum(c['rrf_contribution'] for c in hit['channels'])-hit['score'])<1e-5
        for c in hit['channels']:
            if c['channel']=='relation':assert c['raw_score'] is None and c['rank_basis']=='stable_adjacency'

    bad=fused();bad['relation_fusion']['neighbors']['max_candidates']=1
    call('POST',endpoint+'/query',bad,status=400)
    bad['relation_fusion']['neighbors']['allow_partial']=True
    assert not call('POST',endpoint+'/query',bad)['fusion']['relation_complete']
    bad=fused();bad['relation_fusion']['relation_weight']=0
    call('POST',endpoint+'/query',bad,status=400)

    assert len(call('POST',endpoint+'/neighbors',query('a',direction='out'))['hits'])==1
    call('POST',endpoint+'/neighbors',query('a',per_seed=1),status=400)
    partial=call('POST',endpoint+'/neighbors',query('a',per_seed=1,allow_partial=True))
    assert not partial['complete'] and partial['count']=={'value':1,'relation':'window'}
    bad=edge('cross','a','b');bad['system']['relation']['to']['collection_id']='other'
    call('POST',endpoint+'/doc',bad,status=400)
    bad=edge('stale','a','b');bad['system']['relation']['to']['version']=99
    call('POST',endpoint+'/doc',bad,status=409)
    # A fixed endpoint version must never silently follow a replacement.
    call('POST',endpoint+'/doc',{**a,'operation_id':operation(),'expected':{'Match':1},'document':{'body':'new entity'}})
    assert call('POST',endpoint+'/neighbors',query('a'))['count']['value']==0
    assert call('POST',endpoint+'/query',{})['statistics']['active_records']==2
    call('GET',endpoint+'/doc/ab',status=404)
    call('POST',endpoint+'/doc',edge('new-ab','a','b'))
    archive=call('POST',endpoint+'/backup',{'operation_id':operation()})
    deletion={'operation_id':operation(),'record_id':'b','routing_key':'r','expected':{'Match':1},'document':None}
    call('POST',endpoint+'/doc',deletion)
    assert call('POST',endpoint+'/neighbors',query('a'))['count']['value']==0
    restart_with({})
    assert not any(h['record']['record_id']=='b' for h in call('POST',endpoint+'/query',fused())['hits'])
    assert call('POST',endpoint+'/neighbors',query('a'))['count']['value']==0
    call('POST','/api/v1/restore',{'operation_id':operation(),'collection_id':'relations-copy','backup_id':archive['backup_id']})
    assert call('POST','/api/v1/index/relations-copy/entities',{})['records']==[]
    call('GET','/api/v1/index/relations-copy/doc/new-ab',status=404)
    call('POST',endpoint+'/grants',{'operation_id':operation(),'key_id':'relation-reader','allow':False})
    call('POST',endpoint+'/neighbors',query('a'),key=reader,status=403)
    call('POST',endpoint+'/entities',{},key=reader,status=403)
    call('POST',endpoint+'/query',fused(),key=reader,status=403)
    takeover={'operation_id':operation(),'source_collection_id':'relations','collection_id':'relations-next','backup_id':archive['backup_id']}
    assert call('POST','/api/v1/takeovers',takeover)['status']=='active'
    endpoint='/api/v1/index/relations-next'
    assert call('POST',endpoint+'/neighbors',query('a'))['count']['value']==0
    call('POST',endpoint+'/entities',{},key=reader,status=403)
    call('POST',endpoint+'/query',fused(),key=reader,status=403)
    if faults:
        for stage in ['after_append_unknown','after_durable','crash_after_durable','crash_after_visible']:
            request=edge('self-'+stage,'a','a')
            restart_with({'SEEKPOWER_RECORD_FAULT':stage,'SEEKPOWER_RECORD_FAULT_NONCE':request['operation_id']['nonce']})
            if stage.startswith('crash_'):
                try:call('POST',endpoint+'/doc',request)
                except (OSError,http.client.HTTPException):pass
                else:raise AssertionError('expected graph write process exit')
            else:
                assert call('POST',endpoint+'/doc',request,status=503)['code'] in ('outcome_unknown','durable_pending')
            restart_with({})
            first=call('POST',endpoint+'/doc',request)['receipt']
            assert call('POST',endpoint+'/doc',request)['receipt']==first
            ids=[row['edge']['record_id'] for row in call('POST',endpoint+'/neighbors',query('a'))['hits']]
            assert ids.count(request['record_id'])==1
            fusion=call('POST',endpoint+'/query',fused())
            assert any(h['record']['record_id']=='a' for h in fusion['hits'])
            assert all(h['channels'] for h in fusion['hits'])
            assert not any(h['record']['record_id']=='b' for h in fusion['hits'])
    call('DELETE','/api/v1/backups/'+archive['backup_id'],{'operation_id':operation()})
    call('DELETE','/api/v1/apikey',{'operation_id':operation(),'key_id':'relation-reader'})
    print('PASS relations HTTP: exact endpoints, cycles, bounded counts, update/delete hiding, revocation, quarantine, takeover and replay'+(' with process faults' if faults else ''))
