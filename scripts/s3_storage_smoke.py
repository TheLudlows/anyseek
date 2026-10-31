#!/usr/bin/env python3
"""Real service HTTP acceptance against a fixture or dedicated S3 test bucket.

The default fixture keeps objects in memory and needs no service credentials.
Real mode requires explicit ANYSEEK_S3_TEST_* configuration and a disposable bucket.
"""
import argparse, hashlib, http.server, os, subprocess, sys, threading, urllib.parse

class S3(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    disable_nagle_algorithm = True
    objects = {}
    lock = threading.Lock()
    def log_message(self, *args):
        pass
    def reply(self, status, body=b'', headers=None):
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(body)
    def key(self):
        return urllib.parse.unquote(urllib.parse.urlsplit(self.path).path)
    def do_PUT(self):
        data = self.rfile.read(int(self.headers.get('Content-Length', '0')))
        key = self.key()
        with self.lock:
            if self.headers.get('If-None-Match') == '*' and key in self.objects:
                return self.reply(412, b'<Error><Code>PreconditionFailed</Code></Error>')
            self.objects[key] = data
        self.reply(200, headers={'ETag': '"'+hashlib.md5(data).hexdigest()+'"'})
    def do_GET(self):
        self.read_object()
    def do_HEAD(self):
        self.read_object()
    def read_object(self):
        with self.lock:
            data = self.objects.get(self.key())
        if data is None:
            return self.reply(404, b'<Error><Code>NoSuchKey</Code></Error>')
        self.reply(200, data, {'ETag': '"'+hashlib.md5(data).hexdigest()+'"',
                             'Last-Modified': 'Thu, 22 Oct 2026 00:00:00 GMT'})
    def do_DELETE(self):
        with self.lock:
            self.objects.pop(self.key(), None)
        self.reply(204)

def main():
    import base64, http.client, json, pathlib, shutil, socket, tempfile, time, urllib.error, urllib.request, uuid
    parser=argparse.ArgumentParser();parser.add_argument('--server',required=True);parser.add_argument('--faults',action='store_true');parser.add_argument('--backend',choices=['fixture','real'],default='fixture');args=parser.parse_args()
    binary=str(pathlib.Path(args.server).resolve())
    fixture=None
    if args.backend=='fixture':
        fixture=http.server.ThreadingHTTPServer(('127.0.0.1',0),S3)
        threading.Thread(target=fixture.serve_forever,daemon=True).start()
        env={**os.environ,'ANYSEEK_STORAGE_BACKEND':'s3','ANYSEEK_S3_ENDPOINT':f'http://127.0.0.1:{fixture.server_port}',
             'ANYSEEK_S3_BUCKET':'disposable','ANYSEEK_S3_REGION':'us-east-1','ANYSEEK_S3_ACCESS_KEY_ID':'fixture',
             'ANYSEEK_S3_SECRET_ACCESS_KEY':'fixture-secret','ANYSEEK_S3_ALLOW_HTTP':'1','MASTER_KEY_SECRET':'s3-test'}
    else:
        required=['ENDPOINT','BUCKET','REGION','ACCESS_KEY_ID','SECRET_ACCESS_KEY']
        missing=[f'ANYSEEK_S3_TEST_{key}' for key in required if not os.environ.get(f'ANYSEEK_S3_TEST_{key}')]
        if missing:parser.error('missing dedicated test configuration: '+', '.join(missing))
        if not os.environ.get('ANYSEEK_S3_TEST_RESTORE_BUCKET'):parser.error('set ANYSEEK_S3_TEST_RESTORE_BUCKET to a second disposable bucket for full-service restore')
        env={**os.environ,'ANYSEEK_STORAGE_BACKEND':'s3','MASTER_KEY_SECRET':'s3-test'}
        for key in required+['ALLOW_HTTP']:
            env[f'ANYSEEK_S3_{key}']=os.environ.get(f'ANYSEEK_S3_TEST_{key}','0')
    master=base64.b64encode(hashlib.sha256(b's3-test').digest()).decode()
    tenant_key=base64.b64encode(hashlib.sha256(b's3-tenant').digest()).decode()
    default_key=object()
    def operation():return {'epoch':int(time.time()//86400),'nonce':uuid.uuid4().hex}
    with tempfile.TemporaryDirectory(prefix='anyseek-s3-',dir=os.environ.get('ANYSEEK_TEST_ROOT')) as directory:
        directory=pathlib.Path(directory);root=directory/'data';log=open(directory/'service.log','w+')
        clock=directory/'clock';clock.write_text(str(int(time.time()*1000)));env['ANYSEEK_TEST_CLOCK_FILE']=str(clock)
        process=None;children=[]
        sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close();base=f'http://127.0.0.1:{port}'
        def start(extra=None,target=None,listen=None):
            nonlocal process,base
            base=f'http://127.0.0.1:{listen or port}'
            process=subprocess.Popen([binary,f'index_path={target or root}',f'local_port={listen or port}',
                'local_ip=127.0.0.1'],env={**env,**(extra or {})},stdout=log,stderr=log)
            children.append(process)
            for _ in range(2400):
                if process.poll() is not None:
                    log.seek(0);raise AssertionError(log.read())
                try:urllib.request.urlopen(base+'/api/v1/live',timeout=1).read();return
                except (OSError,urllib.error.URLError):time.sleep(.05)
            log.seek(0);raise AssertionError('S3 service startup timed out: '+log.read()[-8192:])
        def stop():
            if process and process.poll() is None:process.kill();process.wait(timeout=30)
        def call(method,path,body=None,status=200,key=default_key):
            if key is default_key:
                management=(path=='/api/v1/tenants' or path in ['/api/v1/service-backups','/api/v1/service-restores'] or path.startswith('/api/v1/catalog/') or (path=='/api/v1/apikey' and method in ['POST','DELETE']) or path.startswith('/api/v1/backups'))
                key=master if management else tenant_key
            if method=='POST' and path=='/api/v1/apikey' and body is not None:body={'tenant_id':'test',**body}
            req=urllib.request.Request(base+path,data=None if body is None else json.dumps(body).encode(),
                headers={'apikey':key,'Content-Type':'application/json'},method=method)
            try:
                with urllib.request.urlopen(req,timeout=60) as response:code=response.status;raw=response.read()
            except urllib.error.HTTPError as error:code=error.code;raw=error.read()
            assert code in (status if isinstance(status,tuple) else (status,)),(method,path,code,status,raw)
            return json.loads(raw)
        def control(method,path,body):
            deadline=time.monotonic()+300
            while True:
                try:
                    result=call(method,path,body,status=(200,429,503))
                except (OSError,urllib.error.URLError):
                    if process.poll() is not None:
                        log.seek(0);raise AssertionError('control transport lost; service exited '+str(process.returncode)+': '+log.read()[-8192:])
                    assert time.monotonic()<deadline,('control transport retry deadline',path)
                    time.sleep(.1)
                    continue
                if result.get('code') not in ['outcome_unknown','busy']:
                    assert 'code' not in result,(path,result)
                    return result
                assert time.monotonic()<deadline,('control retry deadline',path)
                print('RETRY original control identity: '+path,flush=True)
                # The original owned task retains its class reservation after
                # the HTTP deadline. Never create another operation identity.
                time.sleep(.1)
        def cold_restart():
            stop()
            for path in [*root.glob('collections/*/*/*'), *root.glob('*.object-cache')]:
                if path.is_dir():shutil.rmtree(path)
            start()
        def query(collection='main'):
            result=[]
            for request in [{'query':'alpha','operator':'or'},{'query_vector':[1,0],'all':True},
                            {'query':'alpha','query_vector':[1,0],'rrf':{}}]:
                page=call('POST',f'/api/v1/index/{collection}/query',request)
                result.append((page['count_total'],[(h['record']['record_id'],h['record']['version'],h['record']['document'],h['score']) for h in page['hits']]))
            return result
        def write(nonce,record,body,expected='Absent'):
            return {'operation_id':operation(),'record_id':record,'expected':expected,
                'document':{'body':body,'vector':[2,0]}}
        try:
            start()
            call('POST','/api/v1/tenants',{'operation_id':operation(),'tenant_id':'test'})
            call('POST','/api/v1/apikey',{'operation_id':operation(),'key_id':'test-default','apikey_base64':tenant_key})
            config={'index_name':'S3','schema':[{'field':'body','field_type':'Text'},
                {'field':'vector','field_type':'Json','index_vector':True}],'vector':{'dimensions':2}}
            call('POST','/api/v1/index',{'operation_id':operation(),'collection_id':'main','embedding_space_id':'fixture','config':config})
            original=write('first','a','alpha old');receipt=call('POST','/api/v1/index/main/doc',original)['receipt']
            call('POST','/api/v1/index/main/doc',write('b','b','alpha beta'))
            call('PATCH','/api/v1/index/main')
            call('POST','/api/v1/index/main/doc',write('update','a','alpha updated',{'Match':1}))
            deletion=write('delete','b','',{'Match':1});deletion['document']=None
            call('DELETE','/api/v1/index/main/doc',deletion)
            call('POST','/api/v1/index/main/doc',write('tail','c','alpha tail'))
            expected=query();cold_restart();assert query()==expected
            assert call('POST','/api/v1/index/main/doc',original)['receipt']==receipt
            call('GET','/api/v1/index/main/doc/b',status=404)
            print('PASS S3 checkpoint + unindexed tail: cold cache, update/delete, lexical/vector/hybrid and original receipt',flush=True)
            # Same collection name in another tenant must select different S3 objects.
            call('POST','/api/v1/tenants',{'operation_id':operation(),'tenant_id':'other'})
            other_key=base64.b64encode(os.urandom(32)).decode()
            call('POST','/api/v1/apikey',{'operation_id':operation(),'tenant_id':'other','key_id':'other','apikey_base64':other_key})
            call('POST','/api/v1/index',{'operation_id':operation(),'collection_id':'main','embedding_space_id':'fixture','config':config},key=other_key)
            assert call('GET','/api/v1/index/main',key=other_key)['statistics']['active_records']==0
            call('POST','/api/v1/index/main/doc',write('foreign','a','foreign tenant'),key=other_key)
            assert call('GET','/api/v1/index/main/doc/a',key=other_key)['version']==1
            assert call('GET','/api/v1/index/main/doc/a')['version']==2
            foreign_backup=call('POST','/api/v1/index/main/backup',{'operation_id':operation()},key=other_key)
            call('POST','/api/v1/restore',{'operation_id':operation(),'collection_id':'forbidden','backup_id':foreign_backup['backup_id']},status=403)
            cold_restart()
            assert call('GET','/api/v1/index/main/doc/a',key=other_key)['document']['body']=='foreign tenant'
            assert call('GET','/api/v1/index/main/doc/a')['version']==2
            print('PASS S3 Tenant isolation: same-name collections, separate objects, backup ownership and cold restart',flush=True)
            # Reuse the public name and operation ID, never its old lifecycle objects.
            old_write=write('old-generation','same','old incarnation')
            call('POST','/api/v1/index/main/doc',old_write,key=other_key)
            deleted={'operation_id':operation()}
            call('DELETE','/api/v1/index/main',deleted,key=other_key)
            call('POST','/api/v1/index',{'operation_id':operation(),'collection_id':'main','embedding_space_id':'fixture','config':config},key=other_key)
            fresh={**old_write,'document':{'body':'new incarnation','vector':[2,0]}}
            assert call('POST','/api/v1/index/main/doc',fresh,key=other_key)['receipt']['version']==1
            call('DELETE','/api/v1/index/main',deleted,key=other_key)
            cold_restart()
            assert call('GET','/api/v1/index/main/doc/same',key=other_key)['document']['body']=='new incarnation'
            if fixture:
                with S3.lock:keys=list(S3.objects)
                assert any('/anyseek/other/main/wal/' in k for k in keys)
                assert all('/partitions/' not in k for k in keys)
            assert not root.joinpath('s3-namespace').exists()
            print('PASS S3 same-name recreation: separate lifecycle objects/receipts, stale delete retry and cold restart',flush=True)

            if args.faults:
                for stage in ['before_publish','after_publish']:
                    stop();start({'ANYSEEK_REMOTE_CHECKPOINT_FAULT':stage})
                    try:call('PATCH','/api/v1/index/main')
                    except (OSError,http.client.HTTPException):pass
                    else:raise AssertionError('expected checkpoint process exit')
                    assert process.wait(timeout=30)==91
                    cold_restart();assert query()==expected
                print('PASS S3 checkpoint real process exits before/after SQLite publication',flush=True)
                stop();start({'ANYSEEK_CACHE_APPEND_FAULT':'1'})
                cache_write=write('cache-failure','cache-failure','alpha remotely committed')
                pending=call('POST','/api/v1/index/main/doc',cache_write,status=503)
                assert pending['code']=='durable_pending',pending
                call('POST','/api/v1/index/main/query',{},status=503)
                cold_restart()
                assert call('POST','/api/v1/index/main/doc',cache_write)['receipt']['version']==1
                assert call('GET','/api/v1/index/main/doc/cache-failure')['document']['body']=='alpha remotely committed'
                expected=query()
                print('PASS confirmed SQLite commit + failed local cache append: durable_pending, fenced output, cold recovery and original receipt',flush=True)
            clock.write_text(str(int(clock.read_text())+8*86400000))
            assert query()==expected
            cold_restart();assert query()==expected
            print('PASS remote history maintenance and query views survive cache loss',flush=True)
            stages=['after_snapshot','after_inventory','after_upload','after_descriptor'] if args.faults else ['normal']
            for stage in stages:
                print('S3 joint backup boundary: '+stage,flush=True)
                request={'operation_id':operation()}
                before=call('GET','/api/v1/index/main')['statistics']['visible_seq']
                if stage!='normal':
                    stop();start({'ANYSEEK_REMOTE_BACKUP_FAULT':stage})
                    try:call('POST','/api/v1/index/main/backup',request)
                    except (OSError,http.client.HTTPException):pass
                    else:raise AssertionError('expected backup process exit')
                    assert process.wait(timeout=30)==92;start()
                    call('POST','/api/v1/index/main/doc',write(stage,'later-'+stage,'alpha later'))
                    call('PATCH','/api/v1/index/main')
                archive=call('POST','/api/v1/index/main/backup',request)
                assert archive['visible_seq']==before
                assert call('POST','/api/v1/index/main/backup',request)==archive
                target='restored-'+stage;restore={'operation_id':operation(),'collection_id':target,'backup_id':archive['backup_id']}
                if args.faults:
                    stop();start({'ANYSEEK_REMOTE_RESTORE_FAULT':'after_import'})
                    try:call('POST','/api/v1/restore',restore)
                    except (OSError,http.client.HTTPException):pass
                    else:raise AssertionError('expected restore process exit')
                    assert process.wait(timeout=30)==93;start()
                call('POST','/api/v1/restore',restore)
                assert call('GET',f'/api/v1/index/{target}/doc/a')['version']==2
                assert call('POST',f'/api/v1/index/{target}/doc',original)['receipt']==receipt
                retired=call('DELETE','/api/v1/backups/'+archive['backup_id'],{'operation_id':operation()})
                assert not retired['cleanup_pending'],retired
                call('PATCH','/api/v1/index/main');cold_restart()
                assert call('GET',f'/api/v1/index/{target}/doc/a')['version']==2
            print('PASS joint backup retry, interrupted restore, generation isolation, retirement and cold restart',flush=True)
            # Keep a collection backup in the complete-service archive.
            archive=call('POST','/api/v1/index/main/backup',{'operation_id':operation()})
            user=base64.b64encode(os.urandom(32)).decode()
            call('POST','/api/v1/apikey',{'operation_id':operation(),'key_id':'reader','apikey_base64':user})
            whole_request={'operation_id':operation()}
            whole=control('POST','/api/v1/service-backups',whole_request)
            assert whole['snapshot']['collection_backups']
            assert control('POST','/api/v1/service-backups',whole_request)==whole
            restored=control('POST','/api/v1/service-restores',{'operation_id':operation(),'backup_id':whole['backup_id']})
            target_root=pathlib.Path(restored['target_root']);stop()
            env['ANYSEEK_S3_BUCKET'] = 'disposable-restored' if args.backend == 'fixture' else os.environ['ANYSEEK_S3_TEST_RESTORE_BUCKET']
            start(target=target_root)
            call('GET','/api/v1/index/main',key=user,status=403)
            tenant_key=base64.b64encode(os.urandom(32)).decode()
            call('POST','/api/v1/apikey',{'operation_id':operation(),'key_id':'restored-access','apikey_base64':tenant_key})
            assert call('GET','/api/v1/index/main/doc/a')['version']==2
            # Old collection archives in a service snapshot remain usable.
            copied=next(iter(whole['snapshot']['collection_backups']))
            call('POST','/api/v1/restore',{'operation_id':operation(),'collection_id':'copied-archive','backup_id':copied})
            assert call('GET','/api/v1/index/copied-archive/doc/a')['version']==2
            stop()
            for path in [*target_root.glob('collections/*/*/*'), *target_root.glob('*.object-cache')]:
                if path.is_dir():shutil.rmtree(path)
            start(target=target_root)
            assert call('GET','/api/v1/index/main/doc/a')['version']==2
            assert call('GET','/api/v1/index/copied-archive/doc/a')['version']==2
            assert not target_root.joinpath('s3-namespace').exists()
            print('PASS complete service archive: retained S3 backups, isolated restore, disabled old keys and cold restart',flush=True)
        finally:
            for child in children:
                if child.poll() is None:child.kill();child.wait(timeout=30)
            log.close()
            if fixture:fixture.shutdown();fixture.server_close()
if __name__=='__main__':main()
