"""Bounded selection only; does not start a service or alter engine limits."""
import argparse,gzip,hashlib,heapq,json,pathlib
p=argparse.ArgumentParser();p.add_argument('--corpus',type=pathlib.Path,required=True);p.add_argument('--out',type=pathlib.Path,required=True);a=p.parse_args();a.out.mkdir(parents=True,exist_ok=False)
queries=json.loads(pathlib.Path(__file__).with_name('queries-393.json').read_text());required={d for q in queries for d in q['qrels']};seed='anyseek-capacity-20261025-v1'
def digest(d):return hashlib.sha256((seed+':'+d).encode()).hexdigest()
m=json.loads((a.corpus/'manifest.json').read_text());retained={};heap=[];checked=[]
for entry in m['files']:
 path=a.corpus/entry['name'];h=hashlib.sha256()
 with path.open('rb') as f:
  for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
 assert h.hexdigest()==entry['sha256'] and path.stat().st_size==entry['size_bytes']
 count=0
 with gzip.open(path,'rt') as f:
  for line in f:
   r=json.loads(line);count+=1;d=r['docid']
   if d in required:retained[d]=r;continue
   score=int(digest(d),16)
   if len(heap)<10001:heapq.heappush(heap,(-score,d,r))
   elif score < -heap[0][0]:heapq.heapreplace(heap,(-score,d,r))
 assert count==entry['documents'];checked.append({'file':path.name,'sha256':h.hexdigest(),'records':count});print('verified',path.name,count,flush=True)
assert required==retained.keys()
rows=[retained[d] for d in sorted(required,key=digest)]+[r for _,_,r in sorted(heap,key=lambda x:-x[0])][:10001-len(required)]
f=a.out/'corpus-10001.json';f.write_text(json.dumps(rows,ensure_ascii=False)+'\n')
(a.out/'manifest.json').write_text(json.dumps({'seed':seed,'source_revision':m['revision'],'files':checked,'documents':len(rows),'judged_documents':len(required),'sha256':hashlib.sha256(f.read_bytes()).hexdigest(),'query_groups':{'prior40_nonblind':40,'heldout353':353},'selection':'all judged documents first, then globally seeded hash distractors; partial load coverage must be reported'},indent=2)+'\n')
