"""Read-only quality/receipt probes against the preserved capacity-boundary server.

No operation is resubmitted and no checkpoint is requested by this script.
"""
import argparse,json,pathlib,os,math
from urllib.parse import quote
from http_engine import Engine,dump

def main():
 p=argparse.ArgumentParser();p.add_argument('--root',type=pathlib.Path,required=True);a=p.parse_args();root=a.root.resolve();s=json.loads((root/'summary.json').read_text());live=s['live_service']
 cmd=pathlib.Path(f"/proc/{live['pid']}/cmdline").read_bytes().split(b'\0')
 assert ('index_path='+live['data']).encode() in cmd
 out=root/'boundary-validation';out.mkdir(exist_ok=False);os.sched_setaffinity(0,{2,3})
 e=Engine('anyseek','restricted',out);e.port=live['port']
 result={'revision':s['revision'],'binary_sha256':s['binary_sha256'],'corpus_sha256':s['corpus_sha256'],'accepted':s['accepted'],'configuration':s['overrides'],'note':'Stable partial corpus at first boundary; original questions OR, length100, title/text. No writes/checkpoint retries; not a100k result or Meilisearch comparison.'}
 try:
  result['info_before']=e.request('GET','/api/v1/index/bench')[:3]
  assert result['info_before'][1]['statistics']['active_records']==s['accepted']
  expected={};last=None
  for line in (root/'events.jsonl').open():
   r=json.loads(line)
   if r['kind']=='batch':
    last=r
    for item in r['response'].get('results',[]):
     if item.get('receipt',{}).get('seq') in (1,10000,20000,50000):expected[item['receipt']['seq']]=item
  for item in last['response']['results']:
   if item.get('receipt'):expected[item['receipt']['seq']]=item
  checks=[]
  for seq,item in sorted(expected.items()):
   op=item['operation_id'];response=e.request('GET',f"/api/v1/index/bench/operation/{op['epoch']}/{quote(op['nonce'],safe='')}")[:3]
   assert response[1]['receipt']==item['receipt'];checks.append({'expected':item,'observed':response})
  dump(out/'receipts.json',checks);result['matched_original_receipts']=len(checks)
  accepted=set(json.loads((root/'accepted-docids.json').read_text()));queries=json.loads(pathlib.Path(__file__).with_name('queries-393.json').read_text());probes=[]
  with (out/'queries.jsonl').open('w') as f:
   for q in queries:
    positives={d for d,g in q['qrels'].items() if g>0};assert positives<=accepted,(q['qid'],'missing positive')
    code,body,ms,_=e.request('POST','/api/v1/index/bench/query',{'query':q['text'],'operator':'or','length':100,'count':True,'search_fields':['title','text'],'fields':['docid']},required=False)
    row={'qid':q['qid'],'group':q['group'],'status':code,'response':body,'ms':ms};probes.append(row);f.write(json.dumps(row,ensure_ascii=False)+'\n');f.flush()
    if len(probes)%50==0:print('queries',len(probes),'errors',sum(x['status']!=200 for x in probes),flush=True)
  metrics={}
  for group in ['all','prior40_nonblind','heldout353']:
   entries=[(q,r) for q,r in zip(queries,probes) if group=='all' or q['group']==group];totals={k:0.0 for k in ['recall10','recall100','ndcg10','mrr10']};errors=empty=positive_pairs=0
   for q,r in entries:
    positives={d for d,g in q['qrels'].items() if g>0};positive_pairs+=len(positives);ids=[h['record']['document']['docid'] for h in r['response'].get('hits',[])] if r['status']==200 else []
    errors+=r['status']!=200;empty+=r['status']==200 and not ids
    for k in [10,100]:totals['recall'+str(k)]+=len(set(ids[:k])&positives)/len(positives)
    ideal=sum((2**g-1)/math.log2(i+2) for i,g in enumerate(sorted(q['qrels'].values(),reverse=True)[:10]));dcg=sum((2**q['qrels'].get(d,0)-1)/math.log2(i+2) for i,d in enumerate(ids[:10]));totals['ndcg10']+=dcg/ideal if ideal else 0
    totals['mrr10']+=next((1/(i+1) for i,d in enumerate(ids[:10]) if d in positives),0)
   metrics[group]={'queries':len(entries),'positive_pairs':positive_pairs,'all_positives_in_corpus':True,'errors':errors,'error_rate':errors/len(entries),'successful_empty':empty,'successful_empty_over_all_queries':empty/len(entries),**{k:v/len(entries) for k,v in totals.items()}}
  result['metrics']=metrics;result['info_after']=e.request('GET','/api/v1/index/bench')[:3];assert result['info_before'][1]['statistics']['visible_seq']==result['info_after'][1]['statistics']['visible_seq'];result['completed']=True
 except Exception as exc:result['exception']=repr(exc);raise
 finally:dump(out/'result.json',result);e.writer.close();print(json.dumps(result,ensure_ascii=False),flush=True)
if __name__=='__main__':main()
