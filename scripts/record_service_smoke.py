#!/usr/bin/env python3
"""Native process + real HTTP acceptance for the default Record/catalog service.
Uses a fresh data root, never accesses a running user server. No SDK required.
"""
import argparse, base64, hashlib, json, os, pathlib, shutil, socket, subprocess, tempfile, time, urllib.request, urllib.error, uuid

def operation():
    return {'epoch':int(time.time()//86400),'nonce':uuid.uuid4().hex}

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--server',required=True);parser.add_argument('--startup-timeout',type=float,default=15.0,help='local service readiness deadline in seconds (cold S3 uses at least 120)');parser.add_argument('--faults',action='store_true');parser.add_argument('--cold-restarts',action='store_true');parser.add_argument('--section',choices=['base','locator','graph','temporal','history','indexer','authorization','ingest']);args=parser.parse_args()
    binary=str(pathlib.Path(args.server).resolve())
    with tempfile.TemporaryDirectory(prefix='anyseek-service-', dir=os.environ.get('ANYSEEK_TEST_ROOT')) as tmp:
        root=pathlib.Path(tmp)/'data'
        sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
        base=f'http://127.0.0.1:{port}'
        master=base64.b64encode(hashlib.sha256(b'issue57-service-test').digest()).decode()
        tenant_key=base64.b64encode(hashlib.sha256(b'issue98-tenant-test').digest()).decode()
        default_key=object()
        log=open(pathlib.Path(tmp)/'server.log','w+')
        process=None
        def start(fault_env=None):
            nonlocal process
            if args.cold_restarts:
                assert os.environ.get('ANYSEEK_STORAGE_BACKEND')=='s3','cold-restarts requires the S3 backend'
                if root.exists():
                    for cache in [*root.glob('collections/*/*/*'), *root.glob('*.object-cache')]:
                        if cache.is_dir():shutil.rmtree(cache)
            process=subprocess.Popen([binary,f'index_path={root}',f'local_port={port}','local_ip=127.0.0.1'],env={**os.environ,'MASTER_KEY_SECRET':'issue57-service-test',**(fault_env or {})},stdout=log,stderr=log)
            started=time.monotonic()
            for _ in range(int(max(args.startup_timeout,120 if args.cold_restarts else 0)/.05)):
                if process.poll() is not None:
                    log.seek(0);raise AssertionError(log.read())
                try:
                    urllib.request.urlopen(base+'/api/v1/live',timeout=1).read()
                    elapsed=time.monotonic()-started
                    if elapsed>5:print(f'READY after {elapsed:.3f}s',flush=True)
                    return
                except (OSError,urllib.error.URLError):time.sleep(.05)
            log.seek(0);raise AssertionError('server startup timeout: '+log.read()[-8192:])
        def call(method,path,body=None,key=default_key,status=200):
            if key is default_key:
                management=(path=='/api/v1/tenants' or path in ['/api/v1/service-backups','/api/v1/service-restores'] or path.startswith('/api/v1/catalog/') or (path=='/api/v1/apikey' and method in ['POST','DELETE']) or path.startswith('/api/v1/backups'))
                key=master if management else tenant_key
            if method=='POST' and path=='/api/v1/apikey' and body is not None:
                body={'tenant_id':'test',**body}
            req=urllib.request.Request(base+path,data=None if body is None else json.dumps(body).encode(),headers={'apikey':key,'Content-Type':'application/json'},method=method)
            control_retry=status==200 and path in ['/api/v1/service-backups','/api/v1/service-restores']
            deadline=time.monotonic()+300
            while True:
                try:
                    with urllib.request.urlopen(req,timeout=40) as response:
                        code=response.status;raw=response.read()
                except urllib.error.HTTPError as error:
                    code=error.code;raw=error.read()
                except (OSError,urllib.error.URLError):
                    # A reset can lose either the request or its response. Retry
                    # only these idempotent controls and preserve the exact body.
                    if not control_retry or process.poll() is not None:
                        if control_retry and process.poll() not in [None,87,88]:
                            log.seek(0);print('control transport lost; service exited '+str(process.returncode)+': '+log.read()[-8192:],flush=True)
                        raise
                    assert time.monotonic()<deadline,('control transport retry deadline',path)
                    time.sleep(.1)
                    continue
                result=json.loads(raw)
                if control_retry and (code,result.get('code')) in [(503,'outcome_unknown'),(429,'busy')]:
                    assert time.monotonic()<deadline,('control retry deadline',path)
                    time.sleep(.1)
                    continue
                assert code in (status if isinstance(status,tuple) else (status,)),(method,path,code,status,raw)
                return result

        def restart():
            process.kill();process.wait(timeout=20);start()
        def restart_with(fault_env):
            if process.poll() is None:process.kill();process.wait(timeout=20)
            start(fault_env)
        def section(name):
            import importlib
            exercise=importlib.import_module('record_'+name+'_smoke').exercise
            if name == 'ingest':
                exercise(call,restart_with,root.parent,args.faults,base,tenant_key)
            elif name in ['temporal','history','indexer','authorization']:
                exercise(call,restart_with,root.parent,args.faults)
            else:exercise(call,restart_with,args.faults)
        try:
            if args.faults:
                for stage in ['metadata_committed','root_published']:
                    child=subprocess.Popen([binary,f'index_path={root}',f'local_port={port}','local_ip=127.0.0.1'],env={**os.environ,'MASTER_KEY_SECRET':'issue57-service-test','ANYSEEK_CATALOG_INITIAL_FAULT':stage},stdout=log,stderr=log)
                    try:
                        assert child.wait(timeout=30)==87,stage
                    finally:
                        if child.poll() is None:child.kill();child.wait(timeout=20)
            start()
            call('POST','/api/v1/tenants',{'operation_id':operation(),'tenant_id':'test'})
            call('POST','/api/v1/apikey',{'operation_id':operation(),'key_id':'tenant-default','tenant_id':'test','apikey_base64':tenant_key})
            if args.section and args.section!='base':
                section(args.section)
                return
            # A second owner and a changed bootstrap secret must never rebuild this root.
            for secret in ['issue57-service-test','wrong-master']:
                child=subprocess.Popen([binary,f'index_path={root}',f'local_port={port+1}','local_ip=127.0.0.1'],env={**os.environ,'MASTER_KEY_SECRET':secret},stdout=log,stderr=log)
                assert child.wait(timeout=30)!=0

            keys={name:base64.b64encode(os.urandom(32)).decode() for name in ['a','b']}
            for name,key in keys.items():call('POST','/api/v1/apikey',{'operation_id':operation(),'key_id':name,'apikey_base64':key})
            create={'operation_id':operation(),'collection_id':'shared','config':{'index_name':'Shared records','schema':[{'field':'body','field_type':'Text','store':True},{'field':'title','field_type':'Text','store':True},{'field':'date','field_type':'Timestamp','store':True,'filterable':True}]}}
            assert call('POST','/api/v1/index',create,key=keys['a'])['collection_id']=='shared'
            assert call('POST','/api/v1/index',create,key=keys['a'])['collection_id']=='shared'
            call('GET','/api/v1/index/shared',key=keys['b'])
            writes=[]
            for i,(body,date) in enumerate([('alpha beta',2),('alpha',1),('gamma',3)]):
                w={'operation_id':operation(),'record_id':f'r{i}','expected':'Absent','document':{'body':body,'title':f'Title {i}','date':date,'unindexed':{'original':[1,2,3]}}}
                writes.append(w);r=call('POST','/api/v1/index/shared/doc',w,key=keys['a']);assert r['receipt']['version']==1
            assert call('POST','/api/v1/index/shared/doc',writes[0],key=keys['b'])['receipt']['version']==1
            q=call('POST','/api/v1/index/shared/query',{'query':'alpha beta'},key=keys['b']);assert q['count_total']==1 and q['hits'][0]['record']['record_id']=='r0',q
            q=call('POST','/api/v1/index/shared/query',{'query':'alpha beta','operator':'or'},key=keys['b']);assert q['count_total']==2,q
            q=call('POST','/api/v1/index/shared/query',{'query':'alpha','length':0},key=keys['b']);assert q['count_total']==2 and q['hits']==[],q
            q=call('POST','/api/v1/index/shared/query',{'query':'','sort':[{'field':'date','order':'Ascending'}],'offset':1,'length':1,'fields':['title']},key=keys['b']);assert q['hits'][0]['record']['record_id']=='r0' and q['hits'][0]['record']['document']=={'title':'Title 0'},q
            q=call('POST','/api/v1/index/shared/query',{'query':'','filter':[{'Timestamp':{'field':'date','filter':{'start':2,'end':3}}}]},key=keys['b']);assert [h['record']['record_id'] for h in q['hits']]==['r0'],q
            call('POST','/api/v1/index/shared/doc',{**writes[0],'operation_id':operation(),'routing_key':'obsolete'},key=keys['b'],status=400)
            replacement={**writes[0],'operation_id':operation(),'expected':{'Match':1},'document':{'body':'delta','title':'Replacement','date':2}}
            receipt=call('PATCH','/api/v1/index/shared/doc',replacement,key=keys['b'])['receipt'];assert receipt['version']==2 and receipt['record_id']=='r0'
            stale={**replacement,'operation_id':operation()};assert call('PATCH','/api/v1/index/shared/doc',stale,key=keys['a'],status=409)['code']=='conflict'
            # Lose the entire HTTP response. Retry only the original identity;
            # a status miss is deliberately not interpreted as known failure.
            uncertain={**writes[0],'operation_id':operation(),'record_id':'response-loss','expected':'Absent'}
            raw=json.dumps(uncertain).encode();client=socket.create_connection(('127.0.0.1',port));client.sendall(f'POST /api/v1/index/shared/doc HTTP/1.1\r\nHost: localhost\r\napikey: {keys["a"]}\r\nContent-Type: application/json\r\nContent-Length: {len(raw)}\r\nConnection: close\r\n\r\n'.encode()+raw);client.close()
            confirmed=call('POST','/api/v1/index/shared/doc',uncertain,key=keys['b'])['receipt'];assert confirmed['version']==1
            call('PATCH','/api/v1/index/shared',key=keys['b'])
            restart()
            assert call('GET','/api/v1/index/shared/doc/r1',key=keys['b'])['document']['unindexed']=={'original':[1,2,3]}
            oid=replacement['operation_id'];assert call('GET',f'/api/v1/index/shared/operation/{oid["epoch"]}/{oid["nonce"]}',key=keys['b'])['receipt']==receipt
            rev={'operation_id':operation(),'key_id':'a'};call('DELETE','/api/v1/apikey',rev)
            call('GET','/api/v1/apikey',key=keys['a'],status=403)
            call('GET',f'/api/v1/index/shared/operation/{oid["epoch"]}/{oid["nonce"]}',key=keys['a'],status=403)
            assert call('GET','/api/v1/index/shared/doc/r0',key=keys['b'])['version']==2
            restart();call('GET','/api/v1/index/shared',key=keys['a'],status=403)
            assert call('GET','/api/v1/index/shared',key=keys['b'])['indexed_doc_count']==4
            deletion={**replacement,'operation_id':operation(),'expected':{'Match':2},'document':None}
            assert call('DELETE','/api/v1/index/shared/doc',deletion,key=keys['b'])['receipt']['version']==3
            call('GET','/api/v1/index/shared/doc/r0',key=keys['b'],status=404)
            recreate={**writes[0],'operation_id':operation(),'expected':'Absent'}
            assert call('POST','/api/v1/index/shared/doc',recreate,key=keys['b'])['receipt']['version']==4
            cross_collection={**recreate,'source_collection':'other'}
            call('POST','/api/v1/index/shared/doc',cross_collection,key=keys['b'],status=400)
            future={**writes[0],'operation_id':{'epoch':int(time.time()//86400)+1,'nonce':'future'}}
            assert call('POST','/api/v1/index/shared/doc',future,key=keys['b'],status=410)['code']=='operation_expired'
            # Invalid collection names are rejected before data lookup.
            call('GET','/api/v1/index/0',key=keys['b'],status=400)
            vc={'operation_id':operation(),'collection_id':'vectors','embedding_space_id':'fixture-f32-cosine','config':{'index_name':'Vectors','schema':[{'field':'body','field_type':'Text'},{'field':'vector','field_type':'Json','index_vector':True}],'vector':{'dimensions':2}}}
            call('POST','/api/v1/index',vc)
            vw={'operation_id':operation(),'record_id':'v','expected':'Absent','document':{'body':'alpha','vector':[2,0]}}
            call('POST','/api/v1/index/vectors/doc',vw)
            for query in [{'query_vector':[1,0],'all':True},{'query':'alpha','query_vector':[1,0],'rrf':{}}]:
                q=call('POST','/api/v1/index/vectors/query',query);assert q['hits'][0]['record']['record_id']=='v',q
                hit=q['hits'][0];channels=hit['channels'];assert all(c['rank']==0 and c['raw_score'] is not None for c in channels)
                if query.get('rrf') is not None:
                    assert {c['channel'] for c in channels}=={'lexical','vector'}
                    assert abs(sum(c['rrf_contribution'] for c in channels)-hit['score'])<1e-5
                else:assert channels[0]['channel']=='vector' and channels[0]['raw_score']==hit['score']

            assert call('GET','/api/v1/index/vectors/doc/v')['document']['vector']==[2,0]
            pending={**vw,'operation_id':operation(),'record_id':'pending','document':{'body':'pendingword'}}
            call('POST','/api/v1/index/vectors/doc',pending)
            assert call('POST','/api/v1/index/vectors/query',{'query':'pendingword'})['count_total']==1
            assert len(call('POST','/api/v1/index/vectors/query',{'query_vector':[1,0],'all':True})['hits'])==1
            assert call('GET','/api/v1/index/vectors/doc/pending/status')['pending_vector_fields']==['vector']
            patch={'operation_id':operation(),'record_id':'pending','expected':{'Match':1},'vector_patch':{'vector':[2,0]}}
            enriched=call('POST','/api/v1/index/vectors/doc',patch)['receipt']
            assert enriched['version']==1
            assert call('POST','/api/v1/index/vectors/doc',patch)['receipt']==enriched
            call('POST','/api/v1/index/vectors/doc',{**patch,'operation_id':operation()},status=409)
            assert call('GET','/api/v1/index/vectors/doc/pending/status')['pending_vector_fields']==[]
            assert len(call('POST','/api/v1/index/vectors/query',{'query_vector':[1,0],'all':True})['hits'])==2
            print('PASS optional vector HTTP: lexical before vector, exact version append-only enrichment, retry and readiness')
            vector_backup=call('POST','/api/v1/index/vectors/backup',{'operation_id':operation()})
            call('POST','/api/v1/restore',{'operation_id':operation(),'collection_id':'restored-vectors','backup_id':vector_backup['backup_id']})
            assert call('GET','/api/v1/index/restored-vectors/doc/v')['document']['vector']==[2,0]
            assert len(call('POST','/api/v1/index/restored-vectors/query',{'query_vector':[1,0],'all':True})['hits'])==2
            if args.faults:
                import http.client
                for stage in ['after_append_unknown','after_durable','crash_after_durable','crash_after_visible']:
                    rid='patch-'+stage
                    call('POST','/api/v1/index/vectors/doc',{**pending,'operation_id':operation(),'record_id':rid})
                    enrichment={**patch,'operation_id':operation(),'record_id':rid}
                    process.kill();process.wait(timeout=20)
                    start({'ANYSEEK_RECORD_FAULT':stage,'ANYSEEK_RECORD_FAULT_NONCE':enrichment['operation_id']['nonce']})
                    if stage.startswith('crash_'):
                        try:call('POST','/api/v1/index/vectors/doc',enrichment)
                        except (OSError,http.client.HTTPException):pass
                        else:raise AssertionError('expected vector patch transport loss')
                        process.wait(timeout=20);assert process.returncode==86
                    else:
                        assert call('POST','/api/v1/index/vectors/doc',enrichment,status=503)['code'] in ['outcome_unknown','durable_pending']
                    restart()
                    assert call('POST','/api/v1/index/vectors/doc',enrichment)['receipt']['version']==1
                    assert call('GET','/api/v1/index/vectors/doc/'+rid+'/status')['pending_vector_fields']==[]
                print('PASS vector enrichment HTTP faults: exact body version and receipt across uncertain/pending/process exits')

            # New Record iterator, planned conditional deletion, independent backup and lifecycle.
            page=call('POST','/api/v1/index/shared/iterator',{'length':2,'fields':['title']},key=keys['b'])
            assert len(page['records'])==2 and page['next'] and page['statistics']['active_records']==4
            assert all(set(r['document'])=={'title'} for r in page['records'])
            second=call('POST','/api/v1/index/shared/iterator',{'cursor':page['next'],'length':2},key=keys['b'])
            assert not second['next'] and not ({r['record_id'] for r in page['records']} & {r['record_id'] for r in second['records']})
            plan=call('POST','/api/v1/index/shared/delete-plan',{'select':'query','query':{'query':'alpha','length':1}},key=keys['b'])
            assert len(plan['targets'])==3
            saved=[{**t,'operation_id':operation()} for t in plan['targets']]
            updated={**recreate,'operation_id':operation(),'expected':{'Match':4},'document':{'body':'replacement survives stale delete'}}
            call('POST','/api/v1/index/shared/doc',updated,key=keys['b'])
            call('POST','/api/v1/index/shared/iterator',{'cursor':page['next'],'length':2},key=keys['b'],status=409)
            results=call('POST','/api/v1/index/shared/batch',{'writes':saved},key=keys['b'])
            assert not results['atomic'] and [r['code'] for r in results['results']].count('conflict')==1
            assert call('POST','/api/v1/index/shared/batch',{'writes':saved},key=keys['b'])==results
            assert call('GET','/api/v1/index/shared/doc/r0',key=keys['b'])['version']==5
            backup_request={'operation_id':operation()}
            backup=call('POST','/api/v1/index/shared/backup',backup_request,key=keys['b'])
            assert call('POST','/api/v1/index/shared/backup',backup_request,key=keys['b'])==backup
            restore={'operation_id':operation(),'collection_id':'restored','backup_id':backup['backup_id']}
            call('POST','/api/v1/restore',restore,key=keys['b'])
            call('POST','/api/v1/restore',restore)
            call('POST','/api/v1/restore',restore)
            call('GET','/api/v1/index/restored',key=keys['b'])
            assert call('GET','/api/v1/index/restored/doc/r0')['version']==5
            oid=saved[-1]['operation_id']
            assert call('GET',f'/api/v1/index/restored/operation/{oid["epoch"]}/{oid["nonce"]}')['receipt']
            # Tombstone version + complete operation ledger survive independent restoration.
            assert call('POST','/api/v1/index/restored/batch',{'writes':saved})==results
            all_plan=call('POST','/api/v1/index/restored/delete-plan',{'select':'all'})
            clear=[{**t,'operation_id':operation()} for t in all_plan['targets']]
            assert all(r['code']=='applied' for r in call('POST','/api/v1/index/restored/batch',{'writes':clear})['results'])
            assert call('GET','/api/v1/index/restored')['indexed_doc_count']==0
            # Retrying backup after source changes still returns its original point.
            assert call('POST','/api/v1/index/shared/backup',backup_request,key=keys['b'])==backup
            deletion_request={'operation_id':operation()}
            call('DELETE','/api/v1/index/shared',deletion_request,key=keys['b'])
            call('DELETE','/api/v1/index/shared',deletion_request)
            call('DELETE','/api/v1/index/shared',deletion_request)
            restart()
            call('GET','/api/v1/index/shared',key=keys['b'],status=403)
            call('POST','/api/v1/index',{**create,'operation_id':operation()},key=keys['b'])
            assert call('GET','/api/v1/index/shared',key=keys['b'])['indexed_doc_count']==0
            old=saved[-1]['operation_id']
            assert call('GET',f"/api/v1/index/shared/operation/{old['epoch']}/{old['nonce']}",key=keys['b'])['receipt'] is None
            call('DELETE','/api/v1/index/shared',deletion_request,key=keys['b'])
            call('GET','/api/v1/index/shared',key=keys['b'])
            call('DELETE','/api/v1/index/shared',{'operation_id':operation()},key=keys['b'])
            call('POST','/api/v1/restore',{**restore,'operation_id':operation(),'collection_id':'after-source-delete'})
            assert call('GET','/api/v1/index/after-source-delete/doc/r0')['version']==5
            call('POST','/api/v1/restore',{**restore,'backup_id':'../escape'},status=400)
            # Source revisions preserve bodies while invalidating direct/indirect evidence.
            call('POST','/api/v1/index',{'operation_id':operation(),'collection_id':'revisions','config':{'index_name':'Revisions','schema':[{'field':'body','field_type':'Text','store':True}]}})
            src={'operation_id':operation(),'record_id':'s','expected':'Absent','document':{'body':'revisionword old'},'system':{'kind':'source'}}
            call('POST','/api/v1/index/revisions/doc',src)
            for rid,parent in [('d','s'),('i','d')]:
                call('POST','/api/v1/index/revisions/doc',{**src,'operation_id':operation(),'record_id':rid,'system':{'kind':'derived','evidence_refs':[{'record_id':parent,'version':1}]}})
            update={**src,'operation_id':operation(),'expected':{'Match':1},'document':{'body':'revisionword new'}}
            receipt=call('POST','/api/v1/index/revisions/doc',update,key=keys['b'])['receipt']
            assert receipt['version']==2
            assert call('POST','/api/v1/index/revisions/doc',update)['receipt']==receipt
            call('POST','/api/v1/index/revisions/doc',{**update,'operation_id':operation()},status=409)
            for rid in ['d','i']:
                status=call('GET','/api/v1/index/revisions/doc/'+rid+'/status',key=keys['b'])
                assert status['visibility']=='stale' and 'document' not in status
                call('GET','/api/v1/index/revisions/doc/'+rid,status=404)
            assert call('POST','/api/v1/index/revisions/query',{'query':'revisionword'})['count_total']==1
            call('PATCH','/api/v1/index/revisions');restart()
            assert call('GET','/api/v1/index/revisions/doc/i/status')['visibility']=='stale'
            assert call('POST','/api/v1/index/revisions/doc',update)['receipt']==receipt
            call('DELETE','/api/v1/index/revisions/doc',{'operation_id':operation(),'record_id':'s','expected':{'Match':2},'document':None})
            assert call('GET','/api/v1/index/revisions/doc/i/status')['visibility']=='deleted'
            assert call('GET','/api/v1/index/revisions/doc/i/status',key=keys['b'])['visibility']=='deleted'
            call('DELETE','/api/v1/index/revisions',{'operation_id':operation()})
            print('PASS source revisions HTTP: conditional update/retry, transitive stale, counts, restart, deletion and status authorization')
            # P2 immutable source/evidence through the same authorized Record API.
            ec={'operation_id':operation(),'collection_id':'evidence','config':{'index_name':'Evidence','schema':[{'field':'body','field_type':'Text','store':True}]}}
            call('POST','/api/v1/index',ec)
            source={'operation_id':operation(),'record_id':'source','expected':'Absent','document':{'body':'evidenceword original'},'system':{'kind':'source'}}
            direct={**source,'operation_id':operation(),'record_id':'direct','system':{'kind':'derived','evidence_refs':[{'record_id':'source','version':1}]}}
            indirect={**direct,'operation_id':operation(),'record_id':'indirect','system':{'kind':'derived','evidence_refs':[{'record_id':'direct','version':1}]}}
            for w in [source,direct,indirect]:call('POST','/api/v1/index/evidence/doc',w,key=keys['b'])
            assert call('POST','/api/v1/index/evidence/query',{'query':'evidenceword'})['count_total']==3
            early=call('POST','/api/v1/index/evidence/backup',{'operation_id':operation()})
            delete={'operation_id':operation(),'record_id':'source','expected':{'Match':1},'document':None}
            receipt=call('DELETE','/api/v1/index/evidence/doc',delete)['receipt']
            assert call('DELETE','/api/v1/index/evidence/doc',delete,key=keys['b'])['receipt']==receipt
            for rid in ['source','direct','indirect']:call('GET','/api/v1/index/evidence/doc/'+rid,key=keys['b'],status=404)
            assert call('POST','/api/v1/index/evidence/query',{'query':'evidenceword'},key=keys['b'])['hits']==[]
            assert call('POST','/api/v1/index/evidence/iterator',{},key=keys['b'])['statistics']['active_records']==0
            call('POST','/api/v1/index/evidence/doc',{**source,'operation_id':operation()},status=400)
            call('POST','/api/v1/index/evidence/doc',{**direct,'operation_id':operation(),'record_id':'late'},status=409)
            badscope={**direct,'operation_id':operation(),'record_id':'bad','system':{'kind':'derived','evidence_refs':[{'collection_id':'other','record_id':'source','version':1}]}}
            call('POST','/api/v1/index/evidence/doc',badscope,status=400)
            call('PATCH','/api/v1/index/evidence');restart()
            assert call('POST','/api/v1/index/evidence/query',{},key=keys['b'])['hits']==[]
            restored_evidence=call('POST','/api/v1/restore',{'operation_id':operation(),'collection_id':'evidence-old','backup_id':early['backup_id']})
            assert restored_evidence['evidence_quarantined'] is True
            call('GET','/api/v1/index/evidence-old/doc/source',status=404)
            assert call('POST','/api/v1/index/evidence-old/query',{})['hits']==[]
            print('PASS source HTTP: shared keys, precise transitive evidence, deletion/read/query/iterator, checkpoint restart, old snapshot quarantine')
            if args.faults:
                import http.client
                for stage in ['after_append_unknown','after_durable','crash_after_durable','crash_after_visible']:
                    sid='source-'+stage;did='derived-'+stage
                    call('POST','/api/v1/index/evidence/doc',{**source,'operation_id':operation(),'record_id':sid})
                    call('POST','/api/v1/index/evidence/doc',{**direct,'operation_id':operation(),'record_id':did,'system':{'kind':'derived','evidence_refs':[{'record_id':sid,'version':1}]}})
                    deletion={**delete,'operation_id':operation(),'record_id':sid}
                    process.kill();process.wait(timeout=20)
                    start({'ANYSEEK_RECORD_FAULT':stage,'ANYSEEK_RECORD_FAULT_NONCE':deletion['operation_id']['nonce']})
                    if stage.startswith('crash_'):
                        try:call('DELETE','/api/v1/index/evidence/doc',deletion)
                        except (OSError,http.client.HTTPException):pass
                        else:raise AssertionError('expected source delete transport loss')
                        process.wait(timeout=20);assert process.returncode==86
                    else:
                        assert call('DELETE','/api/v1/index/evidence/doc',deletion,status=503)['code'] in ['outcome_unknown','durable_pending']
                        call('GET','/api/v1/index/evidence/doc/'+did,status=503)
                    restart();call('GET','/api/v1/index/evidence/doc/'+did,status=404)
                    assert call('DELETE','/api/v1/index/evidence/doc',deletion)['receipt']['version']==2
                print('PASS source HTTP faults: unknown/durable pending/process loss recover deletion and retry without derived resurrection')
                for stage in ['after_append_unknown','after_durable','crash_after_durable','crash_after_visible']:
                    sid='revision-'+stage;did='revision-derived-'+stage
                    call('POST','/api/v1/index/evidence/doc',{**source,'operation_id':operation(),'record_id':sid})
                    call('POST','/api/v1/index/evidence/doc',{**direct,'operation_id':operation(),'record_id':did,'system':{'kind':'derived','evidence_refs':[{'record_id':sid,'version':1}]}})
                    revision={**source,'operation_id':operation(),'record_id':sid,'expected':{'Match':1},'document':{'body':'revised content'}}
                    process.kill();process.wait(timeout=20)
                    start({'ANYSEEK_RECORD_FAULT':stage,'ANYSEEK_RECORD_FAULT_NONCE':revision['operation_id']['nonce']})
                    if stage.startswith('crash_'):
                        try:call('POST','/api/v1/index/evidence/doc',revision)
                        except (OSError,http.client.HTTPException):pass
                        else:raise AssertionError('expected source update transport loss')
                        process.wait(timeout=20);assert process.returncode==86
                    else:
                        assert call('POST','/api/v1/index/evidence/doc',revision,status=503)['code'] in ['outcome_unknown','durable_pending']
                    restart()
                    assert call('POST','/api/v1/index/evidence/doc',revision)['receipt']['version']==2
                    assert call('GET','/api/v1/index/evidence/doc/'+did+'/status')['visibility']=='stale'
                    call('GET','/api/v1/index/evidence/doc/'+did,status=404)
                print('PASS source revision HTTP faults: unknown/durable pending/process loss preserve stale and exact retry')

            if args.faults:
                import http.client
                for stage in ['before_append','after_append_unknown','after_durable','crash_after_durable','crash_after_visible']:
                    w={'operation_id':operation(),'record_id':stage,'expected':'Absent','document':{'body':'fault recovery'}}
                    later={**w,'operation_id':operation(),'record_id':stage+'-later'}
                    process.kill();process.wait(timeout=20)
                    start({'ANYSEEK_RECORD_FAULT':stage,'ANYSEEK_RECORD_FAULT_NONCE':w['operation_id']['nonce']})
                    if stage.startswith('crash_'):
                        try: call('POST','/api/v1/index/after-source-delete/doc',w)
                        except (OSError,http.client.HTTPException): pass
                        else: raise AssertionError('expected transport loss at '+stage)
                        process.wait(timeout=20);assert process.returncode==86
                    else:
                        result=call('POST','/api/v1/index/after-source-delete/batch',{'writes':[w,later]})['results']
                        expected={'before_append':'rejected','after_append_unknown':'outcome_unknown','after_durable':'durable_pending'}[stage]
                        assert result[0]['code']==expected,result
                        if stage=='before_append':
                            assert result[1]['code']=='applied'
                        else:
                            assert result[1]['code']=='not_attempted'
                            call('POST','/api/v1/index/after-source-delete/query',{},status=503)
                    restart()
                    oid=w['operation_id'];status=call('GET',f'/api/v1/index/after-source-delete/operation/{oid["epoch"]}/{oid["nonce"]}')
                    assert bool(status['receipt']) == (stage!='before_append'),(stage,status)
                    first=call('POST','/api/v1/index/after-source-delete/doc',w)['receipt']
                    assert first['version']==1 and call('POST','/api/v1/index/after-source-delete/doc',w)['receipt']==first
                    if stage!='before_append':call('GET','/api/v1/index/after-source-delete/doc/'+later['record_id'],status=404)
                # Independent snapshots are fully certified in a private root.
                # Exercise both sides of its final atomic publication. Initial
                # root publication/repair crashes remain covered at startup.
                for kind in ([] if args.cold_restarts else ['backup','restore']):
                    for stage in ['prepared','published']:
                        body={'operation_id':operation()}
                        endpoint='/api/v1/index/after-source-delete/backup'
                        restored_id=f'interrupted-restore-{stage}'
                        if kind=='restore':
                            body.update({'collection_id':restored_id,'backup_id':backup['backup_id']});endpoint='/api/v1/restore'
                        process.kill();process.wait(timeout=20)
                        start({'ANYSEEK_SNAPSHOT_ROOT_PREFIX':'backup-' if kind=='backup' else 'collections','ANYSEEK_SNAPSHOT_FAULT':stage})
                        try: call('POST',endpoint,body)
                        except (OSError,http.client.HTTPException):pass
                        else:raise AssertionError(('expected snapshot publication exit',kind,stage))
                        process.wait(timeout=20);assert process.returncode==87,(kind,stage,process.returncode)
                        start();recovered=call('POST',endpoint,body)
                        assert call('POST',endpoint,body)==recovered,(kind,stage,'idempotent retry')
                        if kind=='restore':assert call('GET',f'/api/v1/index/{restored_id}/doc/r0')['version']==5
                        else:assert recovered['backup_id']
                print('PASS real HTTP faults: rejection/unknown/durable pending, process exits before/after visibility and stopped batch tail'+('; local backup/restore private and published snapshot crashes' if not args.cold_restarts else '; remote backup faults run in the storage suite'))
            if args.cold_restarts and args.faults:
                while sum(p.name.startswith('backup-') for p in root.iterdir())<31:
                    call('POST','/api/v1/index/after-source-delete/backup',{'operation_id':operation()})
                pointer_request={'operation_id':operation()}
                pointer_cut=call('GET','/api/v1/index/after-source-delete')['statistics']['visible_seq']
                for stage in ['after_directory','after_marker_sync']:
                    restart_with({'ANYSEEK_BACKUP_POINTER_FAULT':stage})
                    try:call('POST','/api/v1/index/after-source-delete/backup',pointer_request)
                    except (OSError,http.client.HTTPException):pass
                    else:raise AssertionError('expected pointer process exit')
                    assert process.wait(timeout=30)==94
                start()
                repaired=call('POST','/api/v1/index/after-source-delete/backup',pointer_request)
                assert repaired['visible_seq']==pointer_cut
                assert call('POST','/api/v1/index/after-source-delete/backup',pointer_request)==repaired
                assert sum(p.name.startswith('backup-') for p in root.iterdir())==32
                assert not list(root.glob('backup-*/.marker-*'))
                print('PASS S3 pointer slot 32: directory/marker-sync exits resume original backup and clean private metadata')
            # Rejecting excess backup requests must not allocate unbounded repair files.
            while sum(p.name.startswith('backup-') for p in root.iterdir()) < 32:
                call('POST','/api/v1/index/after-source-delete/backup',{'operation_id':operation()})
            before={p.name for p in root.iterdir()}
            for _ in range(3):call('POST','/api/v1/index/after-source-delete/backup',{'operation_id':operation()},status=400)
            assert {p.name for p in root.iterdir()}==before
            # Retirement is durable and never lets an original backup ID capture a new point.
            call('GET','/api/v1/backups',key=keys['b'],status=403)
            listed=call('GET','/api/v1/backups');assert any(x['backup_id']==backup['backup_id'] for x in listed)
            call('DELETE','/api/v1/backups/'+backup['backup_id'].upper(),{'operation_id':operation()},status=400)
            call('POST','/api/v1/restore',{'operation_id':operation(),'collection_id':'case-restore','backup_id':backup['backup_id'].upper()},status=400)
            retire={'operation_id':operation()}
            result=call('DELETE','/api/v1/backups/'+backup['backup_id'],retire)
            assert result['retired'] and not result['cleanup_pending'],result
            call('POST','/api/v1/restore',restore) # Original completed restore stays retryable.
            call('POST','/api/v1/restore',{'operation_id':operation(),'collection_id':'retired-restore','backup_id':backup['backup_id']},status=410)
            restart()
            assert any(x['backup_id']==backup['backup_id'] and x['retired'] and not x['on_disk'] for x in call('GET','/api/v1/backups'))
            call('POST','/api/v1/index/after-source-delete/backup',{'operation_id':operation()})
            print('PASS Record lifecycle: stable-view iterator, non-atomic conditional batch/delete plans, backup retries, complete restore, Tenant-scoped restore, same-name recreation and stale delete retry across restart')
            print('PASS real HTTP: default Record/catalog, same-Tenant keys, conditional writes, retry/response loss, checkpoint, crash restart, revoked operation lookup, full query parameters, vectors')
            # Whole service cut, including revoked authority and independent backups.
            ckey=base64.b64encode(os.urandom(32)).decode()
            add_key={'operation_id':operation(),'key_id':'snapshot-user','apikey_base64':ckey}
            call('POST','/api/v1/apikey',add_key)
            whole_request={'operation_id':operation()}
            call('POST','/api/v1/service-backups',whole_request,key=ckey,status=403)
            whole=call('POST','/api/v1/service-backups',whole_request)
            assert whole['snapshot']['collection_backups']
            call('DELETE','/api/v1/apikey',{'operation_id':operation(),'key_id':'snapshot-user'})
            assert call('POST','/api/v1/service-backups',whole_request)==whole
            service_restore={'operation_id':operation(),'backup_id':whole['backup_id']}
            restored=call('POST','/api/v1/service-restores',service_restore)
            assert call('POST','/api/v1/service-restores',service_restore)==restored
            assert not restored['started']
            original_base=base
            other_port=port+2
            restore_env = {}
            if os.environ.get('ANYSEEK_STORAGE_BACKEND') == 's3':
                restore_env['ANYSEEK_S3_BUCKET'] = os.environ['ANYSEEK_S3_TEST_RESTORE_BUCKET']
            restored_process=subprocess.Popen([binary,f"index_path={restored['target_root']}",f'local_port={other_port}','local_ip=127.0.0.1'],env={**os.environ,'MASTER_KEY_SECRET':'issue57-service-test',**restore_env},stdout=log,stderr=log)
            try:
                base=f'http://127.0.0.1:{other_port}'
                for _ in range(int(max(args.startup_timeout,120 if args.cold_restarts else 0)/.05)):
                    if restored_process.poll() is not None:
                        log.seek(0);raise AssertionError(log.read())
                    try:urllib.request.urlopen(base+'/api/v1/live',timeout=1).read();break
                    except (OSError,urllib.error.URLError):time.sleep(.05)
                else:
                    log.seek(0);raise AssertionError('restored service startup timeout: '+log.read()[-8192:])
                call('GET','/api/v1/index/after-source-delete',key=ckey,status=403)
                call('GET','/api/v1/index/after-source-delete',key=keys['b'],status=403)
                original_tenant_key=tenant_key
                restored_key=base64.b64encode(os.urandom(32)).decode()
                call('POST','/api/v1/apikey',{'operation_id':operation(),'key_id':'restored-access','tenant_id':'test','apikey_base64':restored_key})
                tenant_key=restored_key
                call('GET','/api/v1/index/after-source-delete')
                call('GET','/api/v1/index/shared',status=403)
                call('GET','/api/v1/index/evidence/doc/direct',status=404)
                assert call('GET','/api/v1/index/evidence')['evidence_quarantined'] is True
                call('POST','/api/v1/apikey',add_key,status=409)
                old=add_key['operation_id'];call('GET',f"/api/v1/catalog/operation/{old['epoch']}/{old['nonce']}",status=409)
                assert call('POST','/api/v1/index/after-source-delete/doc',writes[0])['receipt']['version']==1
                # A copied independent collection snapshot is usable after root recovery.
                copied=next(iter(whole['snapshot']['collection_backups']))
                call('POST','/api/v1/restore',{'operation_id':operation(),'collection_id':'from-copied-backup','backup_id':copied})
                restored_base=base;base=original_base
                assert call('POST','/api/v1/service-restores',service_restore)==restored
                base=restored_base
            finally:
                restored_process.kill();restored_process.wait(timeout=20);base=original_base;tenant_key=original_tenant_key
            assert call('POST','/api/v1/service-restores',service_restore)==restored # Target was legitimately modified.
            if args.faults:
                for kind in ([] if args.cold_restarts else ['backup','restore']):
                    body={'operation_id':operation()}
                    endpoint='/api/v1/service-backups'
                    if kind=='restore':body['backup_id']=whole['backup_id'];endpoint='/api/v1/service-restores'
                    for stage in [kind+'_children',kind+'_manifest','bundle_published']:
                        process.kill();process.wait(timeout=20);start({'ANYSEEK_SERVICE_SNAPSHOT_FAULT':stage})
                        try:call('POST',endpoint,body);raise AssertionError('expected process exit')
                        except (OSError,urllib.error.URLError):pass
                        assert process.wait(timeout=30)==88,stage
                    start()
                    result=call('POST',endpoint,body)
                    assert call('POST',endpoint,body)==result
                # The completion witness has its own initial-publication boundary.
                for kind in ([] if args.cold_restarts else ['backup','restore']):
                    body={'operation_id':operation()};endpoint='/api/v1/service-backups'
                    if kind=='restore':body['backup_id']=whole['backup_id'];endpoint='/api/v1/service-restores'
                    process.kill();process.wait(timeout=20)
                    start({'ANYSEEK_INITIAL_ROOT_PREFIX':'service-snapshot-complete','ANYSEEK_INITIAL_FAULT':'published'})
                    try:call('POST',endpoint,body);raise AssertionError('expected completion-witness exit')
                    except (OSError,urllib.error.URLError):pass
                    assert process.wait(timeout=30)==87
                    start();result=call('POST',endpoint,body)
                    assert call('POST',endpoint,body)==result
            print('PASS complete service snapshot: common catalog cut, all retained collections/live backups, new-root recovery, no revived keys, historical control rejection, data retry preserved')
            from index_write_query_smoke import exercise as client_exercise
            client_requests=root.parent/'client-requests.json'
            client_exercise(base,tenant_key,client_requests)
            client_exercise(base,tenant_key,client_requests)
            call('DELETE','/api/v1/backups/'+early['backup_id'],{'operation_id':operation()})
            if args.section=='base':return
            section('ingest')
            from record_temporal_smoke import exercise as temporal_exercise
            from record_locator_smoke import exercise as locator_exercise
            locator_exercise(call,restart_with,args.faults)
            from record_graph_smoke import exercise as graph_exercise
            graph_exercise(call,restart_with,args.faults)
            temporal_exercise(call,restart_with,root.parent,args.faults)
            from record_history_smoke import exercise as history_exercise
            history_exercise(call,restart_with,root.parent,args.faults)
        except Exception:
            log.flush();log.seek(0)
            print('SERVER LOG AT FAILURE:\n'+log.read()[-16384:],flush=True)
            raise
        finally:
            if process and process.poll() is None:process.kill();process.wait(timeout=20)
            log.close()
if __name__=='__main__':main()
