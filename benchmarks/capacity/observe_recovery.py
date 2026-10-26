"""Observe a retained timed-out recovery without restarting or changing limits."""
import argparse,json,pathlib,sys,os,time,signal
from urllib.parse import quote
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
from http_engine import Engine,dump
parser=argparse.ArgumentParser();parser.add_argument('--root',type=pathlib.Path,required=True);args=parser.parse_args();root=args.root.resolve();accepted=json.loads((root/'summary.json').read_text())['accepted'];recovery=root/'boundary-recovery';base=json.loads((recovery/'result.json').read_text());live=base['retained_reopen_service'];out=recovery/'continued-observation'
# Keep the first successful readiness observation intact; use a new log directory.
out=out/'final';out.mkdir(parents=True,exist_ok=False);e=Engine('anyseek','restricted',out);e.port=live['port'];result={'initial_failure':base['reopen_exception'],'note':'Read-only continuation of the same retained process; no restart and no changed deadline/budget. Original startup failure remains.'}
try:
 assert base['reopen_exception']=="RuntimeError('startup exceeded 60 seconds')"
 until=time.monotonic()+180
 while True:
  ready=e.request('GET','/api/v1/index/bench',required=False)[:3]
  if ready[0]==200:
   first=recovery/'continued-observation/initial.json'
   if not first.exists():dump(first,{'info':ready,'note':'First successful read-only observation after original startup timeout.'})
   break
  if time.monotonic()>=until:raise RuntimeError('bounded follow-up observation ended; same process retained')
  time.sleep(1)
 result['info']=e.request('GET','/api/v1/index/bench')[:3];assert result['info'][1]['statistics']['active_records']==accepted;assert result['info'][1]['statistics']['retained_operations']==accepted;assert e.count()==accepted
 checks=[]
 for item in json.loads((root/'boundary-validation/receipts.json').read_text()):
  expected=item['expected'];op=expected['operation_id'];r=e.request('GET',f"/api/v1/index/bench/operation/{op['epoch']}/{quote(op['nonce'],safe='')}")[:3];assert r[1]['receipt']==expected['receipt'];checks.append({'expected':expected,'observed':r})
 dump(out/'receipts.json',checks);result['matched_receipts']=len(checks)
 queries={q['qid']:q for q in json.loads(pathlib.Path(__file__).with_name('queries-393.json').read_text())};rows=[json.loads(l) for l in (root/'boundary-validation/queries.jsonl').open()][:5];checks=[]
 for row in rows:
  q=queries[row['qid']];r=e.request('POST','/api/v1/index/bench/query',{'query':q['text'],'operator':'or','length':100,'count':True,'search_fields':['title','text'],'fields':['docid']});project=lambda b:[(h['record']['document']['docid'],h['score']) for h in b['hits']];assert project(r[1])==project(row['response']);assert r[1]['count_total']==row['response']['count_total'];checks.append({'qid':q['qid'],'response':r[1],'ms':r[2]})
 dump(out/'queries.json',checks);result['matched_queries']=len(checks);result['recovery_consistency_completed']=True
 starts=[json.loads(l) for l in (recovery/'reopen/events.jsonl').open() if json.loads(l)['kind']=='start'];first_ready=recovery/'continued-observation/initial.json';result['ready_observed_by_seconds']=first_ready.stat().st_mtime-starts[0]['wall_time'];result['startup_60_seconds_passed']=False
 # Every accepted operation is visible; read-only probes finished and no write was retried.
 cmd=pathlib.Path(f"/proc/{live['pid']}/cmdline").read_bytes().split(b'\0');assert ('index_path='+live['data']).encode() in cmd;os.kill(live['pid'],signal.SIGTERM)
 until=time.monotonic()+30
 while time.monotonic()<until:
  p=pathlib.Path(f"/proc/{live['pid']}/stat")
  if not p.exists() or p.read_text().split()[2]=='Z':result['service_stopped']=True;break
  time.sleep(.1)
except Exception as ex:result['exception']=repr(ex);result['retained_service']=live;raise
finally:dump(out/'result.json',result);e.writer.close();print(json.dumps(result,ensure_ascii=False),flush=True)
