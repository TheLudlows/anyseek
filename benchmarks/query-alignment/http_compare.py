#!/usr/bin/env python3
import sys,os,json,time,random,threading,concurrent.futures as cf,shutil,gzip,functools,hashlib
from pathlib import Path
ROOT=Path(os.environ.get("ANYSEEK_QUERY_BENCH_ROOT",str(Path(__file__).resolve().parent / "results"))).resolve()
ROOT.mkdir(parents=True,exist_ok=True)
import support as b
N=2048;DIM=768;SEED=20261109;EPOCH=int(time.time()//86400);C='bench';READERS=4;QUERIES=120;WRITES=72;BATCH=4
@functools.lru_cache(maxsize=10000)
def doc(i,rev=0):
 r=random.Random(SEED+i*7919+rev*104729)
 return {'body':'common tag'+chr(97+i%16)+' marker'+b.codeword(i+rev*1000000)+' '+' '.join('word'+chr(97+r.randrange(16)) for _ in range(24)),'serial':i,'group':i%16,'vector':[r.randrange(-16,17)/16 for _ in range(DIM)]}
def commands(start,count,rev):
 return [{'operation_id':b.m.oid(EPOCH,f'query-validation-{i}-{rev}'),'record_id':f'r{i:06d}','expected':'Absent' if rev==0 else {'Match':1},'document':doc(i,rev)} for i in range(start,start+count)]
def dump(p,x):p.write_text(json.dumps(x,indent=2)+'\n')
def write(s,rows):
 r,ms,a=b.request(s,'POST',b.path(C)+'/ingest',{'records':rows})
 assert len(r.get('results',[]))==len(rows) and all(x.get('code')=='committed' for x in r['results']),r
 return {'ms':ms,'attempts':a,'last_seq':max(x['receipt']['seq'] for x in r['results'])}
def query(mode):
 q={'query':'common','length':10,'fields':['body','serial','group']}
 if mode=='hybrid':q.update(query_vector=doc(23)['vector'],all=True,rrf={})
 if mode=='facet':q.update(sort=[{'field':'group','order':'Ascending'}])
 return q
def sig(s,mode):
 r,_,_=b.query(s,C,query(mode));assert r['count_total']==N,r
 return {'count':r['count_total'],'hits':[[h['record']['record_id'],h['record']['version'],h['record']['document'],h['score'],h['channels']] for h in r['hits']]}
def seed():
 p=ROOT/'http-seed'
 if p.exists():return p
 s=b.Server(ROOT/'bin/base-server',p,ROOT/'http-seed.log',[0,1])
 try:
  s.call('POST','/api/v1/tenants',{'operation_id':b.m.oid(EPOCH,'tenant'),'tenant_id':'bench'},b.m.MASTER)
  s.call('POST','/api/v1/apikey',{'operation_id':b.m.oid(EPOCH,'key'),'key_id':'bench','tenant_id':'bench','apikey_base64':b.m.KEY},b.m.MASTER)
  config={'index_name':'query-alignment-768','tokenizer':'TantivySimpleV1','schema':[{'field':'body','field_type':'Text','store':True,'index_lexical':True},{'field':'group','field_type':'Int','store':True,'filterable':True,'index_lexical':False},{'field':'serial','field_type':'Int','store':True,'filterable':True,'index_lexical':False},{'field':'vector','field_type':'Json','store':False,'index_lexical':False,'index_vector':True}],'vector':{'dimensions':DIM}}
  s.call('POST','/api/v1/index',{'operation_id':b.m.oid(EPOCH,'collection'),'collection_id':C,'config':config,'capacity_profile':'standard_10k'})
  for i in range(0,N,16):write(s,commands(i,16,0))
  b.wait(s,C,N);b.request(s,'PATCH',b.path(C))
  dump(ROOT/'http-seed-signature.json',{m:sig(s,m) for m in ['lexical','hybrid','facet']})
 finally:s.close()
 dump(ROOT/'http-seed-files.json',b.m.files(p));return p

def run(version,mode,index,out,source):
 out.mkdir();data=out/'db';shutil.copytree(source,data)
 for i in range(WRITES*BATCH):doc(i,1)
 assert b.m.files(data)==json.loads((ROOT/'http-seed-files.json').read_text())
 s=b.Server(ROOT/f'bin/{version}-server',data,out/'server.log',[0,1]);errors=[];reads=[];writes=[];progress=[];success=False;sampler=None
 try:
  initial=sig(s,mode);assert initial==json.loads((ROOT/'http-seed-signature.json').read_text())[mode]
  for _ in range(10):b.query(s,C,query(mode))
  barrier=threading.Barrier(READERS+2);start=[0.0]
  def reader(slot):
   barrier.wait()
   for i in range(QUERIES):
    deadline=start[0]+i*.03;time.sleep(max(0,deadline-time.perf_counter()))
    t=time.perf_counter()
    try:
     r,ms,a=b.query(s,C,query(mode));assert 0<=r['count_total']<=N,r
     assert len({h['record']['record_id'] for h in r['hits']})==len(r['hits'])
     assert all(h['record']['version'] in (1,2) for h in r['hits'])
     reads.append({'slot':slot,'i':i,'ms':ms,'schedule_lag_ms':(t-deadline)*1000,'scheduled_to_completion_ms':(t-deadline)*1000+ms,'count':r['count_total'],'attempts':a})
    except Exception as e:errors.append({'reader':slot,'i':i,'error':repr(e)});return
  def writer():
   barrier.wait()
   for i in range(WRITES):
    deadline=start[0]+i*.05;time.sleep(max(0,deadline-time.perf_counter()))
    try:
     t=time.perf_counter();w=write(s,commands(i*BATCH,BATCH,1));w['schedule_lag_ms']=(t-deadline)*1000;w['scheduled_to_ack_ms']=(time.perf_counter()-deadline)*1000;w['i']=i;w['ack_at_s']=time.perf_counter()-start[0];writes.append(w)
     p,_,_=b.request(s,'GET',b.path(C)+'/ingest');assert not p['recovery_required'] and not p['index_error'],p
     progress.append({'at_s':time.perf_counter()-start[0],**p})
    except Exception as e:errors.append({'writer':i,'error':repr(e)});return
  with b.Sampler(s) as sampler,cf.ThreadPoolExecutor(max_workers=READERS+1) as pool:
   jobs=[pool.submit(reader,i) for i in range(READERS)]+[pool.submit(writer)]
   start[0]=time.perf_counter();barrier.wait()
   for job in jobs:job.result()
   wall=time.perf_counter()-start[0]
   drain=b.wait(s,C,N+WRITES*BATCH) if not errors else None
  assert not errors,errors
  assert len(reads)==READERS*QUERIES and len(writes)==WRITES
  final=sig(s,mode)
  observed=time.perf_counter()-start[0]
  for w in writes:
   times=[p['at_s'] for p in progress if min(p['lexical_seq'],p['vector_seq'])>=w['last_seq']]
   w['public_cursor_after_ack_upper_ms']=(min(times+[observed])-w['ack_at_s'])*1000
  for i in sorted({0,min(71,WRITES*BATCH-1),WRITES*BATCH-1}):
   q={'query':'marker'+b.codeword(i+1000000),'length':1,'fields':['serial']};r,_,_=b.query(s,C,q)
   assert r['count_total']==1 and r['hits'][0]['record']['version']==2,r
   r,_,_=b.query(s,C,{'query':'marker'+b.codeword(i),'length':1});assert r['count_total']==0,r
  attempts=[a for r in reads+writes for a in r['attempts']]
  result={'version':version,'mode':mode,'index':index,'wall_s':wall,'query_count':len(reads),'write_requests':len(writes),'write_records':len(writes)*BATCH,'query_latency':b.stats([r['ms'] for r in reads]),'query_scheduled_latency':b.stats([r['scheduled_to_completion_ms'] for r in reads]),'write_scheduled_latency':b.stats([r['scheduled_to_ack_ms'] for r in writes]),'write_ack':b.stats([r['ms'] for r in writes]),'schedule_lag':b.stats([r['schedule_lag_ms'] for r in reads]),'busy_attempts':sum(a['status']==429 or a['busy_items']>0 for a in attempts),'attempts':len(attempts),'errors':errors,'drain':drain,'resources':sampler.summary(),'initial':initial,'final':final}
  with gzip.open(out/'raw.json.gz','wt') as f:json.dump({'reads':reads,'writes':writes,'progress':progress,'resources':sampler.rows},f)
  dump(out/'summary.json',result);success=True;return result
 except BaseException as e:
  errors.append({'fatal':repr(e)});raise
 finally:
  with gzip.open(out/'raw.json.gz','wt') as f:json.dump({'reads':reads,'writes':writes,'progress':progress,'errors':errors,'success':success,'resources':sampler.rows if sampler else []},f)
  s.close()
  if errors:dump(out/'errors.json',errors)
  # Caller removes data only after cross-version final parity succeeds.
if __name__=='__main__':
 os.sched_setaffinity(0,{2,3});source=seed();out=ROOT/'http';out.mkdir(exist_ok=True)
 rounds=int(sys.argv[1]) if len(sys.argv)>1 else 3
 results=[]
 for mode in ['lexical','hybrid','facet']:
  oracle=None
  for index,version in enumerate(['base','candidate','candidate','base']*rounds):
   r=run(version,mode,index,out/f'{mode}-{index}-{version}',source)
   if oracle is None:oracle=r['final']
   assert oracle==r['final'],'final parity'
   shutil.rmtree(out/f'{mode}-{index}-{version}'/'db')
   results.append(r);dump(out/'results.json',results);print(mode,index,version,r['query_latency'],r['busy_attempts'],flush=True)
