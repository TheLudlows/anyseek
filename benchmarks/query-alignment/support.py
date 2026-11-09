# Disposable local HTTP/proc helpers, extracted from existing validation harnesses.
import sys
import time
import threading
import statistics
from pathlib import Path
HERE=Path(__file__).resolve().parent
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared_record_http import (KEY, MASTER, SECRET, LocalServer, cpu, proc_pid,
                                quantiles, digest, oid, request, files)

class BaseServer(LocalServer):
 allocation_library = HERE / 'alloc.so'

def stats(v):
 if not v:return {'n':0}
 return {'n':len(v),'mean_ms':statistics.mean(v),**quantiles(v),'max_ms':max(v)}

def codeword(n):
 s=''
 for _ in range(6):s+=chr(97+n%26);n//=26
 return s

def path(c):return '/api/v1/index/'+c

class Server(BaseServer):
 @property
 def last_status(self):return getattr(self.local,'status',None)
 @last_status.setter
 def last_status(self,value):self.local.status=value

def wait(s,c,cut):
 start=time.perf_counter(); polls=0
 while time.perf_counter()-start<120:
  p,_,_=request(s,'GET',path(c)+'/ingest');polls+=1
  assert not p['recovery_required'] and not p['index_error'],p
  if min(p['lexical_seq'],p['vector_seq'])>=cut:return {'elapsed_ms':(time.perf_counter()-start)*1000,'polls':polls,'progress':p}
  time.sleep(.005)
 raise TimeoutError(('catchup',c,cut,p))

def query(s,c,q):
 result,elapsed,attempts=request(s,'POST',path(c)+'/query',q)
 assert 'hits' in result,result
 return result,elapsed,attempts

def cgroup():
 return {k:int(v) for k,v in (line.split() for line in Path('/sys/fs/cgroup/cpu.stat').read_text().splitlines())}

class Sampler:
 def __init__(self,s):self.s=s;self.rows=[];self.stop=threading.Event();self.t=threading.Thread(target=self.run,daemon=True)
 def run(self):
  while not self.stop.wait(.05):
   try:self.rows.append({'time':time.perf_counter(),**resource(self.s)})
   except (OSError,KeyError):pass
 def __enter__(self):self.before=resource(self.s);self.cg=cgroup();self.t.start();return self
 def __exit__(self,*args):self.after=resource(self.s);self.endcg=cgroup();self.stop.set();self.t.join()
 def summary(self):
  return {'cpu_s':self.after['cpu_s']-self.before['cpu_s'],'peak_sampled_rss_kib':max([r['rss_kib'] for r in self.rows]+[self.before['rss_kib'],self.after['rss_kib']]),'hwm_kib':self.after['hwm_kib'],
          'io_delta':{k:self.after[k]-self.before[k] for k in ('read_bytes','write_bytes','rchar','wchar','syscr','syscw')},'cgroup_delta':{k:self.endcg[k]-self.cg[k] for k in self.cg}}

def resource(server):
 fields=dict(line.split(':',1) for line in Path(f'/proc/{server.proc_pid}/status').read_text().splitlines() if ':' in line)
 r={'cpu_s':cpu(server.proc_pid),'rss_kib':int(fields['VmRSS'].split()[0]),'hwm_kib':int(fields['VmHWM'].split()[0])}
 for line in Path(f'/proc/{server.proc_pid}/io').read_text().splitlines():
  k,v=line.split(':');r[k]=int(v)
 return r
