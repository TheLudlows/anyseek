#!/usr/bin/env python3
"""Verify adjusted update persistence and HTTP/1 refusal, using PR #82 benchmark helpers.
Set ANYSEEK_BENCHMARK_SCRIPTS to that checkout's benchmarks/miracl-meili directory.
Outputs refuse to overwrite an existing verification directory.
"""
import sys,pathlib,json,os,collections
sys.path.insert(0,os.environ['ANYSEEK_BENCHMARK_SCRIPTS']);import run
run.ROOT=pathlib.Path(__file__).resolve().parents[1];run.ART=run.ROOT/'artifacts/benchmark'
rows=json.loads((run.ART/'data/corpus-1000.json').read_text());docs=[run.doc(r) for r in rows]
out=run.ART/'raw/adjusted-update-verification';out.mkdir();e=run.Engine('anyseek','default',out);e.data=run.ART/'raw/adjusted-default-1000-write/db'
try:
 e.start();assert e.count()==1000;prior=json.loads((run.ART/'raw/adjusted-default-1000-write/run.json').read_text());results=[]
 for d,w in zip(docs,prior['writes']):
  r=e.request('GET',f'/api/v1/index/bench/doc/{d["id"]}')[1]['document'];updated=r['text']==d['text']+' benchmark_update_marker'
  assert updated or r['text']==d['text'];assert not w['ok'] or updated
  results.append({'ack':w['ok'],'persisted':updated})
 run.dump(out/'verification.json',{'records':results,'ack':sum(x['ack'] for x in results),'persisted':sum(x['persisted'] for x in results)})
finally:e.close()
os.environ['ANYSEEK_FOREGROUND_PER_COLLECTION']='1'
out=run.ART/'raw/adjusted-forced-busy-http';out.mkdir();e=run.Engine('anyseek','default',out)
try:
 e.start();e.create();e.write(docs[:20]);q=[{'qid':'probe','text':rows[0]['title']}]
 result=run.load(e,q,8,3,'forced-busy');counts=collections.Counter(json.loads(l)['status'] for l in (out/'forced-busy.jsonl').open())
 assert counts[429]>0,counts
 assert counts[0]==0,counts
 e.write([{**docs[0],'text':docs[0]['text']+' after_refusal'}])
 run.dump(out/'verification.json',{'load':result,'status_counts':dict(counts),'subsequent_write':'applied'})
finally:e.close()
