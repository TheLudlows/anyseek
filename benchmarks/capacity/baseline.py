"""Current-master HTTP capacity boundary; no engine overrides or swallowed rejects."""
import argparse,json,pathlib,os,time,hashlib
from http_engine import Engine,doc,dump,op
from observation import observe_batch
class Current(Engine):
 def create(self):
  cfg={'index_name':'capacity-baseline','schema':[{'field':f,'field_type':'Text','index_lexical':f in ['title','text'],'store':True} for f in ['id','docid','title','text']]}
  self.seq+=1;self.request('POST','/api/v1/index',{'operation_id':op(self.seq),'collection_id':'bench','config':cfg,**({'capacity_profile':self.capacity_profile} if getattr(self,'capacity_profile',None) else {})});dump(self.out/'settings.json',self.request('GET','/api/v1/index/bench')[1])
def main():
 p=argparse.ArgumentParser();p.add_argument('--revision',required=True);p.add_argument('--binary',type=pathlib.Path,required=True);p.add_argument('--data',type=pathlib.Path,required=True);p.add_argument('--out',type=pathlib.Path,required=True);p.add_argument('--capacity-profile',choices=['standard_10k','bounded_100k']);p.add_argument('--take',type=int);p.add_argument('--build-single-mib',type=int);p.add_argument('--build-shared-mib',type=int);a=p.parse_args();a.out=a.out.resolve();a.out.mkdir(parents=True,exist_ok=False)
 assert not any(k.startswith('ANYSEEK_') for k in os.environ);os.sched_setaffinity(0,{2,3});os.environ['TOKENIZER_ANYSEEK_BINARY']=str(a.binary.resolve())
 overrides={}
 for arg,key in [(a.build_single_mib,'ANYSEEK_BUILD_MEMORY_BYTES'),(a.build_shared_mib,'ANYSEEK_BUILD_SHARED_MEMORY_BYTES')]:
  if arg is not None:overrides[key]=str(arg*1024*1024)
 os.environ.update(overrides)
 rows=json.loads(a.data.read_text());rows=rows[:a.take] if a.take else rows;e=Current('anyseek','restricted',a.out);e.capacity_profile=a.capacity_profile;accepted=[];last=None;failed=None;summary={'revision':a.revision,'binary_sha256':hashlib.sha256(a.binary.read_bytes()).hexdigest(),'corpus_sha256':hashlib.sha256(a.data.read_bytes()).hexdigest(),'attempt_target':len(rows),'accepted':0,'attempted':0,'status':'running','overrides':overrides,'capacity_profile':a.capacity_profile,'cpu_max':pathlib.Path('/sys/fs/cgroup/cpu.max').read_text().strip(),'memory_max':pathlib.Path('/sys/fs/cgroup/memory.max').read_text().strip()}
 try:
  e.start();e.create();start=time.monotonic();e.phase='load'
  for i in range(0,len(rows),25):
   space=os.statvfs(a.out);available=space.f_bavail*space.f_frsize
   if available<2*1024**3:
    summary['resource_stop']={'reason':'preserved 2GiB free-space margin','available_bytes':available};break
   writes=[]
   for row in rows[i:i+25]:
    d=doc(row);d.pop('date');e.seq+=1;writes.append({'operation_id':op(e.seq),'record_id':d['id'],'expected':'Any','document':d})
   code,body,ms,_=e.request('POST','/api/v1/index/bench/batch',{'writes':writes},required=False);e.event('batch',offset=i,status=code,ms=ms,response=body);summary['attempted']+=len(writes)
   results=body.get('results',[])
   for j,w in enumerate(writes):
    result=results[j] if j<len(results) else {'missing_result':True,'body':body}
    if code==200 and result.get('code')=='applied':accepted.append(w['document']['docid']);last=(w,result)
    elif failed is None:failed=(w,result);summary['first_failure']={'ordinal':i+j+1,'http_status':code,'result':result,'operation_id':w['operation_id']}
   summary['accepted']=len(accepted);dump(a.out/'summary.json',summary)
   if i%250==0:print('loaded',len(accepted),'attempted',summary['attempted'],flush=True)
   if failed:
    # Preserve the original error and accepted-response count. The owned batch
    # may continue after the HTTP response deadline; never close it immediately.
    e.phase='observe_unknown';summary['load_seconds']=time.monotonic()-start
    summary['leave_service_running']=True  # retain owned work even if observation itself fails
    observation=observe_batch(e,writes);dump(a.out/'unknown-observation.json',observation)
    summary['unknown_observation']={'submitted':observation['submitted'],'observed_receipts':len(observation['observed_receipts']),'all_receipts_observed':observation['all_receipts_observed'],'info':observation['info']}
    summary['leave_service_running']=not observation['all_receipts_observed']
    summary['status']='boundary_observed';dump(a.out/'accepted-docids.json',accepted);return
   if len(accepted) in (10000,20000,50000) and len(accepted)<len(rows):
    e.phase='milestone';milestone={'accepted':len(accepted),'visible_before':e.count()};t=time.monotonic()
    milestone['checkpoint']=e.request('PATCH','/api/v1/index/bench',required=False)[:2];milestone['checkpoint_seconds']=time.monotonic()-t
    summary.setdefault('milestones',[]).append(milestone);dump(a.out/'summary.json',summary)
    if milestone['checkpoint'][0]!=200:
     summary['leave_service_running']=True
     raise RuntimeError(('milestone checkpoint failed; service retained for observation',milestone))
    e.stop();milestone['reopen_ms']=e.start();milestone['visible_after']=e.count()
    milestone['retry']=e.request('POST','/api/v1/index/bench/batch',{'writes':[last[0]]},required=False)[:2]
    assert milestone['visible_after']==len(accepted),(len(accepted),milestone)
    e.phase='load';dump(a.out/'summary.json',summary);print('milestone',json.dumps(milestone),flush=True)
  summary['load_seconds']=time.monotonic()-start;e.phase='query';summary['visible_before']=e.count()
  queries=json.loads(pathlib.Path(__file__).with_name('queries-393.json').read_text());accepted_set=set(accepted)
  summary['qrels_coverage']={group:{'positive_pairs':sum(g>0 for q in queries if q['group']==group for g in q['qrels'].values()),'included_positive_pairs':sum(g>0 and d in accepted_set for q in queries if q['group']==group for d,g in q['qrels'].items())} for group in ['prior40_nonblind','heldout353']}
  probes=[]
  for q in queries:
   status,body,ms,_=e.request('POST','/api/v1/index/bench/query',{'query':q['text'],'operator':'or','length':10,'count':True,'search_fields':['title','text'],'fields':['docid']},required=False)
   probes.append({'qid':q['qid'],'group':q['group'],'status':status,'response':body,'ms':ms})
  dump(a.out/'query-probes.json',probes);summary['query_errors']=sum(r['status']!=200 for r in probes)
  e.phase='checkpoint';t=time.monotonic();summary['checkpoint']=e.request('PATCH','/api/v1/index/bench',required=False)[:2];summary['checkpoint_seconds']=time.monotonic()-t
  if summary['checkpoint'][0]!=200:
   summary['leave_service_running']=True
   raise RuntimeError(('final checkpoint failed; service retained for observation',summary['checkpoint']))
  e.stop();e.phase='reopen';summary['reopen_ms']=e.start();summary['visible_after']=e.count()
  if last:
   summary['accepted_retry']=e.request('POST','/api/v1/index/bench/batch',{'writes':[last[0]]},required=False)[:2];summary['original_accepted_result']=last[1]
  summary['visible_after_retries']=e.count();summary['status']='resource_stopped' if 'resource_stop' in summary else ('boundary_observed' if failed else 'target_loaded');dump(a.out/'accepted-docids.json',accepted)
 except Exception as exc:summary['status']='failed';summary['exception']=repr(exc)
 finally:
  if summary.get('leave_service_running'):
   summary['live_service']={'pid':e.process.pid,'port':e.port,'data':str(e.data),'reason':'unknown work retained; inspect before stopping or retrying'}
   e.stop_sample.set();e.sampler.join(timeout=2);dump(a.out/'resources.json',e.resources);e.writer.close()
  else:e.close()
  summary['peak_rss_kib']=max((x['rss_kib'] for x in e.resources),default=0);dump(a.out/'summary.json',summary);print(json.dumps(summary,ensure_ascii=False),flush=True)
if __name__=='__main__':main()
