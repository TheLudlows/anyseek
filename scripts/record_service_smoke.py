#!/usr/bin/env python3
"""Native process + real HTTP acceptance for the default Record/catalog service.
Uses a fresh data root, never accesses a running user server. No SDK required.
"""
import argparse, base64, hashlib, json, os, pathlib, socket, subprocess, tempfile, time, urllib.request, urllib.error, uuid

def operation():
    return {'epoch':int(time.time()//86400),'nonce':uuid.uuid4().hex}

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--server',required=True);parser.add_argument('--browser',action='store_true');args=parser.parse_args()
    binary=str(pathlib.Path(args.server).resolve())
    with tempfile.TemporaryDirectory(prefix='seekpower-service-', dir=os.environ.get('SEEKPOWER_TEST_ROOT')) as tmp:
        root=pathlib.Path(tmp)/'data'
        sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
        base=f'http://127.0.0.1:{port}'
        master=base64.b64encode(hashlib.sha256(b'issue57-service-test').digest()).decode()
        log=open(pathlib.Path(tmp)/'server.log','w+')
        process=None
        def start():
            nonlocal process
            process=subprocess.Popen([binary,f'index_path={root}',f'local_port={port}','local_ip=127.0.0.1','force_shard_number=1'],env={**os.environ,'MASTER_KEY_SECRET':'issue57-service-test'},stdout=log,stderr=log)
            for _ in range(300):
                if process.poll() is not None:
                    log.seek(0);raise AssertionError(log.read())
                try:
                    urllib.request.urlopen(base+'/api/v1/live',timeout=1).read();return
                except (OSError,urllib.error.URLError):time.sleep(.05)
            raise AssertionError('server startup timeout')
        def call(method,path,body=None,key=master,status=200):
            req=urllib.request.Request(base+path,data=None if body is None else json.dumps(body).encode(),headers={'apikey':key,'Content-Type':'application/json'},method=method)
            try:
                with urllib.request.urlopen(req,timeout=30) as r: code=r.status;raw=r.read()
            except urllib.error.HTTPError as e:code=e.code;raw=e.read()
            assert code==status,(method,path,code,status,raw)
            return json.loads(raw)
        def restart():
            process.kill();process.wait(timeout=20);start()
        try:
            start()
            keys={name:base64.b64encode(os.urandom(32)).decode() for name in ['a','b']}
            for name,key in keys.items():call('POST','/api/v1/apikey',{'operation_id':operation(),'key_id':name,'apikey_base64':key})
            create={'operation_id':operation(),'collection_id':'shared','config':{'index_name':'Shared records','schema':[{'field':'body','field_type':'Text','store':True},{'field':'title','field_type':'Text','store':True},{'field':'date','field_type':'Timestamp','store':True,'filterable':True}]}}
            assert call('POST','/api/v1/index',create,key=keys['a'])['collection_id']=='shared'
            assert call('POST','/api/v1/index',create,key=keys['a'])['collection_id']=='shared'
            call('GET','/api/v1/index/shared',key=keys['b'],status=403)
            grant={'operation_id':operation(),'key_id':'b','allow':True}
            call('POST','/api/v1/index/shared/grants',grant,key=keys['a'],status=403)
            call('POST','/api/v1/index/shared/grants',grant)
            writes=[]
            for i,(body,date) in enumerate([('alpha beta',2),('alpha',1),('gamma',3)]):
                w={'operation_id':operation(),'record_id':f'r{i}','routing_key':'route','expected':'Absent','document':{'body':body,'title':f'Title {i}','date':date,'unindexed':{'original':[1,2,3]}}}
                writes.append(w);r=call('POST','/api/v1/index/shared/doc',w,key=keys['a']);assert r['receipt']['version']==1
            assert call('POST','/api/v1/index/shared/doc',writes[0],key=keys['b'])['receipt']['version']==1
            q=call('POST','/api/v1/index/shared/query',{'query':'alpha beta'},key=keys['b']);assert q['count_total']==1 and q['hits'][0]['record']['record_id']=='r0',q
            q=call('POST','/api/v1/index/shared/query',{'query':'alpha beta','operator':'or'},key=keys['b']);assert q['count_total']==2,q
            q=call('POST','/api/v1/index/shared/query',{'query':'alpha','length':0},key=keys['b']);assert q['count_total']==2 and q['hits']==[],q
            q=call('POST','/api/v1/index/shared/query',{'query':'','sort':[{'field':'date','order':'Ascending'}],'offset':1,'length':1,'fields':['title']},key=keys['b']);assert q['hits'][0]['record']['record_id']=='r0' and q['hits'][0]['record']['document']=={'title':'Title 0'},q
            q=call('POST','/api/v1/index/shared/query',{'query':'','filter':[{'Timestamp':{'field':'date','filter':{'start':2,'end':3}}}]},key=keys['b']);assert [h['record']['record_id'] for h in q['hits']]==['r0'],q
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
            wrong_route={**recreate,'operation_id':operation(),'routing_key':'different','expected':'Any'}
            call('POST','/api/v1/index/shared/doc',wrong_route,key=keys['b'],status=409)
            cross_collection={**recreate,'source_collection':'other'}
            call('POST','/api/v1/index/shared/doc',cross_collection,key=keys['b'],status=400)
            future={**writes[0],'operation_id':{'epoch':int(time.time()//86400)+1,'nonce':'future'}}
            assert call('POST','/api/v1/index/shared/doc',future,key=keys['b'],status=410)['code']=='operation_expired'
            # Unknown scopes must never resolve through legacy per-key indices.
            call('GET','/api/v1/index/0',key=keys['b'],status=403)
            vc={'operation_id':operation(),'collection_id':'vectors','embedding_space_id':'fixture-f32-cosine','config':{'index_name':'Vectors','schema':[{'field':'body','field_type':'Text'},{'field':'vector','field_type':'Json','index_vector':True}],'vector':{'dimensions':2}}}
            call('POST','/api/v1/index',vc)
            vw={'operation_id':operation(),'record_id':'v','routing_key':'v','expected':'Absent','document':{'body':'alpha','vector':[2,0]}}
            call('POST','/api/v1/index/vectors/doc',vw)
            for query in [{'query_vector':[1,0],'all':True},{'query':'alpha','query_vector':[1,0],'rrf':{}}]:
                q=call('POST','/api/v1/index/vectors/query',query);assert q['hits'][0]['record']['record_id']=='v',q
            assert call('GET','/api/v1/index/vectors/doc/v')['document']['vector']==[2,0]
            print('PASS real HTTP: default Record/catalog, shared grants, conditional writes, retry/response loss, checkpoint, crash restart, revoked operation lookup, full query parameters, vectors')
            if args.browser:
                from record_service_browser import exercise
                exercise(base,master,call)
        finally:
            if process and process.poll() is None:process.kill();process.wait(timeout=20)
            log.close()
if __name__=='__main__':main()
