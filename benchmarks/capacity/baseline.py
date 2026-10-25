"""Current-master HTTP capacity boundary; no engine overrides or swallowed rejects."""
import argparse,json,pathlib,os,time,hashlib
from http_engine import Engine,doc,dump,op
class Current(Engine):
 def create(self):
  cfg={'index_name':'capacity-baseline','schema':[{'field':f,'field_type':'Text','index_lexical':f in ['title','text'],'store':True} for f in ['id','docid','title','text']]}
  self.seq+=1;self.request('POST','/api/v1/index',{'operation_id':op(self.seq),'collection_id':'bench','config':cfg});dump(self.out/'settings.json',self.request('GET','/api/v1/index/bench')[1])
def main():
 p=argparse.ArgumentParser();p.add_argument('--revision',required=True);p.add_argument('--binary',type=pathlib.Path,required=True);p.add_argument('--data',type=pathlib.Path,required=True);p.add_argument('--out',type=pathlib.Path,required=True);a=p.parse_args();a.out=a.out.resolve();a.out.mkdir(parents=True,exist_ok=False)
 assert not any(k.startswith('ANYSEEK_') for k in os.environ);os.sched_setaffinity(0,{2,3});os.environ['TOKENIZER_ANYSEEK_BINARY']=str(a.binary.resolve())
 rows=json.loads(a.data.read_text());e=Current('anyseek','restricted',a.out);accepted=[];last=None;failed=None;summary={'revision':a.revision,'binary_sha256':hashlib.sha256(a.binary.read_bytes()).hexdigest(),'corpus_sha256':hashlib.sha256(a.data.read_bytes()).hexdigest(),'attempt_target':len(rows),'accepted':0,'attempted':0,'status':'running','overrides':{},'cpu_max':pathlib.Path('/sys/fs/cgroup/cpu.max').read_text().strip(),'memory_max':pathlib.Path('/sys/fs/cgroup/memory.max').read_text().strip()}
 try:
  e.start();e.create();start=time.monotonic();e.phase='load'
  for i in range(0,len(rows),25):
   writes=[]
   for row in rows[i:i+25]:
    d=doc(row);d.pop('date');e.seq+=1;writes.append({'operation_id':op(e.seq),'record_id':d['id'],'routing_key':'benchmark','expected':'Any','document':d})
   code,body,ms,_=e.request('POST','/api/v1/index/bench/batch',{'writes':writes},required=False);e.event('batch',offset=i,status=code,ms=ms,response=body);summary['attempted']+=len(writes)
   results=body.get('results',[])
   for j,w in enumerate(writes):
    result=results[j] if j<len(results) else {'missing_result':True,'body':body}
    if code==200 and result.get('code')=='applied':accepted.append(w['document']['docid']);last=(w,result)
    elif failed is None:failed=(w,result);summary['first_failure']={'ordinal':i+j+1,'http_status':code,'result':result,'operation_id':w['operation_id']}
   summary['accepted']=len(accepted);dump(a.out/'summary.json',summary)
   if i%250==0:print('loaded',len(accepted),'attempted',summary['attempted'],flush=True)
   if failed:break
  summary['load_seconds']=time.monotonic()-start;e.phase='query';summary['visible_before']=e.count()
  queries=json.loads(pathlib.Path(__file__).with_name('queries-393.json').read_text());accepted_set=set(accepted)
  summary['qrels_coverage']={group:{'positive_pairs':sum(g>0 for q in queries if q['group']==group for g in q['qrels'].values()),'included_positive_pairs':sum(g>0 and d in accepted_set for q in queries if q['group']==group for d,g in q['qrels'].items())} for group in ['prior40_nonblind','heldout353']}
  probes=[]
  for q in queries:
   status,body,ms,_=e.request('POST','/api/v1/index/bench/query',{'query':q['text'],'operator':'or','length':10,'count':True,'search_fields':['title','text'],'fields':['docid']},required=False)
   probes.append({'qid':q['qid'],'group':q['group'],'status':status,'response':body,'ms':ms})
  dump(a.out/'query-probes.json',probes);summary['query_errors']=sum(r['status']!=200 for r in probes)
  e.phase='checkpoint';t=time.monotonic();summary['checkpoint']=e.request('PATCH','/api/v1/index/bench',required=False)[:2];summary['checkpoint_seconds']=time.monotonic()-t
  e.stop();e.phase='reopen';summary['reopen_ms']=e.start();summary['visible_after']=e.count()
  if last:
   summary['accepted_retry']=e.request('POST','/api/v1/index/bench/batch',{'writes':[last[0]]},required=False)[:2];summary['original_accepted_result']=last[1]
  if failed:summary['failed_retry']=e.request('POST','/api/v1/index/bench/batch',{'writes':[failed[0]]},required=False)[:2]
  summary['visible_after_retries']=e.count();summary['status']='boundary_observed' if failed else 'target_loaded';dump(a.out/'accepted-docids.json',accepted)
 except Exception as exc:summary['status']='failed';summary['exception']=repr(exc)
 finally:
  e.close();summary['peak_rss_kib']=max((x['rss_kib'] for x in e.resources),default=0);dump(a.out/'summary.json',summary);print(json.dumps(summary,ensure_ascii=False),flush=True)
if __name__=='__main__':main()
