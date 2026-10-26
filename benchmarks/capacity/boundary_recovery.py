"""One explicit post-boundary checkpoint, then same-budget reopen; never retry writes."""
import argparse,json,pathlib,os,time,signal,hashlib
from urllib.parse import quote
from http_engine import Engine,dump

def main():
 p=argparse.ArgumentParser();p.add_argument('--root',type=pathlib.Path,required=True);p.add_argument('--binary',type=pathlib.Path,required=True);a=p.parse_args();root=a.root.resolve();s=json.loads((root/'summary.json').read_text());live=s['live_service'];assert json.loads((root/'boundary-validation/result.json').read_text())['completed'];assert hashlib.sha256(a.binary.read_bytes()).hexdigest()==s['binary_sha256']
 out=root/'boundary-recovery';out.mkdir(exist_ok=False);os.sched_setaffinity(0,{2,3});e=Engine('anyseek','restricted',out);e.port=live['port'];result={'revision':s['revision'],'binary_sha256':s['binary_sha256'],'configuration':s['overrides'],'original_live_service':live,'note':'One new checkpoint after all batch results/receipt observation and393 queries; no failed write is resubmitted. Original boundary evidence is preserved.'}
 def save():dump(out/'result.json',result)
 try:
  result['before']=e.request('GET','/api/v1/index/bench')[:3];assert result['before'][1]['statistics']['visible_seq']==s['accepted'];save()
  result['checkpoint']=e.request('PATCH','/api/v1/index/bench',required=False)[:3];save();code,body,_=result['checkpoint']
  if code!=200 and (code==0 or body.get('code')=='outcome_unknown' or 'record build resource budget' not in str(body)):
   result['retained_original_service']=True;return
  result['after_checkpoint']=e.request('GET','/api/v1/index/bench')[:3];assert result['after_checkpoint'][1]['statistics']['visible_seq']==s['accepted'];save()
  cmd=pathlib.Path(f"/proc/{live['pid']}/cmdline").read_bytes().split(b'\0');assert ('index_path='+live['data']).encode() in cmd
  os.kill(live['pid'],signal.SIGTERM)
  until=time.monotonic()+30
  while time.monotonic()<until:
   p=pathlib.Path(f"/proc/{live['pid']}/stat")
   if not p.exists() or p.read_text().split()[2]=='Z':break
   time.sleep(.1)
  else:raise RuntimeError('original process did not exit; no force kill or second server started')
  result['original_service_stopped']=True;save();e.writer.close()
  os.environ.update(s['overrides']);os.environ['TOKENIZER_ANYSEEK_BINARY']=str(a.binary.resolve());reopen=out/'reopen';reopen.mkdir();n=Engine('anyseek','restricted',reopen);n.data=pathlib.Path(live['data'])
  try:
   result['reopen_ms']=n.start();result['reopened_info']=n.request('GET','/api/v1/index/bench')[:3];assert result['reopened_info'][1]['statistics']['active_records']==s['accepted'];assert result['reopened_info'][1]['statistics']['retained_operations']==s['accepted']
   result['reopened_count']=n.count();assert result['reopened_count']==s['accepted'];checks=[]
   for item in json.loads((root/'boundary-validation/receipts.json').read_text()):
    expected=item['expected'];op=expected['operation_id'];r=n.request('GET',f"/api/v1/index/bench/operation/{op['epoch']}/{quote(op['nonce'],safe='')}")[:3];assert r[1]['receipt']==expected['receipt'];checks.append({'expected':expected,'observed':r})
   dump(out/'reopened-receipts.json',checks);result['matched_reopened_receipts']=len(checks)
   queries={q['qid']:q for q in json.loads(pathlib.Path(__file__).with_name('queries-393.json').read_text())};before=[json.loads(l) for l in (root/'boundary-validation/queries.jsonl').open()][:5];after=[]
   for row in before:
    q=queries[row['qid']];r=n.request('POST','/api/v1/index/bench/query',{'query':q['text'],'operator':'or','length':100,'count':True,'search_fields':['title','text'],'fields':['docid']});ids=lambda body:[(h['record']['document']['docid'],h['score']) for h in body['hits']];assert ids(r[1])==ids(row['response']);assert r[1]['count_total']==row['response']['count_total'];after.append({'qid':q['qid'],'response':r[1],'ms':r[2]})
   dump(out/'reopened-query-probes.json',after);result['matched_reopened_queries']=len(after);result['completed']=True
  except Exception as exc:result['reopen_exception']=repr(exc)
  finally:
   if not result.get('completed') and n.process and n.process.poll() is None:
    result['retained_reopen_service']={'pid':n.process.pid,'port':n.port,'data':str(n.data)};n.stop_sample.set();n.sampler.join(timeout=2);dump(reopen/'resources.json',n.resources);n.writer.close()
   else:n.close()
 except Exception as exc:result['exception']=repr(exc)
 finally:save();e.writer.close();print(json.dumps(result,ensure_ascii=False),flush=True)
if __name__=='__main__':main()
