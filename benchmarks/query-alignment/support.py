# Disposable local HTTP/proc helpers, extracted from existing validation harnesses.
import os,sys,json,time,threading,math,statistics,hashlib,http.client,socket,subprocess,base64,resource as usage
from pathlib import Path
SECRET='disposable-issue153-benchmark'
MASTER=base64.b64encode(hashlib.sha256(SECRET.encode()).digest()).decode()
KEY=base64.b64encode(hashlib.sha256(b'issue153-tenant').digest()).decode()
HERE=Path(__file__).resolve().parent
m=sys.modules[__name__]
def cpu(pid):
    values = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return (int(values[11]) + int(values[12])) / os.sysconf('SC_CLK_TCK')

def proc_pid(binary, data):
    # Some managed executors expose host /proc while subprocess uses namespace PIDs.
    # Resolve the exact disposable server by two unique command-line arguments.
    wanted = {str(binary).encode(), f'index_path={data}'.encode()}
    matches = []
    for path in Path('/proc').glob('[0-9]*'):
        try:
            if wanted.issubset(set((path/'cmdline').read_bytes().split(b'\0'))):
                matches.append(int(path.name))
        except (OSError, PermissionError):
            pass
    if len(matches) != 1:
        raise RuntimeError(('ambiguous server /proc identity', matches))
    return matches[0]

def quantiles(values):
    values = sorted(values)
    return {f'p{p}_ms': values[math.ceil(len(values)*p/100)-1] for p in (50, 95, 99)}

class BaseServer:
    def __init__(self, binary, data, log, cpus, allocation_file=None):
        self.child_usage_before = usage.getrusage(usage.RUSAGE_CHILDREN)
        self.local = threading.local()
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]
        temporary = data.parent/'tmp'
        temporary.mkdir(exist_ok=True)
        env = {**os.environ, 'TMPDIR': str(temporary), 'MASTER_KEY_SECRET': SECRET,
               'TOKIO_WORKER_THREADS': '2', 'RAYON_NUM_THREADS': '2', 'RUST_LOG': 'anyseek::record::maintenance=info', 'ANYSEEK_STORAGE_BACKEND': 'local'}
        if allocation_file:
            env.update(LD_PRELOAD=str(HERE/'alloc.so'), ISSUE153_ALLOC_FILE=str(allocation_file))
        self.log = log.open('w')
        self.p = subprocess.Popen(['taskset', '-c', ','.join(map(str, cpus)), str(binary),
                                   f'index_path={data}', f'local_port={self.port}',
                                   'local_ip=127.0.0.1'], env=env, stdout=self.log, stderr=self.log)
        try:
            deadline = time.monotonic()+90
            while time.monotonic() < deadline:
                if self.p.poll() is not None:
                    raise RuntimeError(f'server exited: {log.read_text()[-4000:]}')
                try:
                    self.call('GET', '/api/v1/live')
                    self.proc_pid = proc_pid(binary, data)
                    return
                except (OSError, http.client.HTTPException):
                    time.sleep(.05)
            raise TimeoutError('startup')
        except BaseException:
            self.close()
            raise

    def close(self):
        started = time.perf_counter()
        if self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait()
                raise RuntimeError("shutdown timed out")
        self.log.close()
        assert self.p.returncode == 0, self.p.returncode
        used = usage.getrusage(usage.RUSAGE_CHILDREN)
        self.total_cpu_s = used.ru_utime+used.ru_stime-self.child_usage_before.ru_utime-self.child_usage_before.ru_stime
        self.shutdown_ms = (time.perf_counter()-started)*1000

    def call(self, method, path, body=None, key=KEY, allowed=(200,)):
        conn = getattr(self.local, 'connection', None)
        if conn is None:
            conn = self.local.connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=120)
        payload = None if body is None else json.dumps(body, separators=(',', ':')).encode()
        started = time.perf_counter_ns()
        try:
            conn.request(method, path, payload, {'Content-Type': 'application/json', 'apikey': key})
            response = conn.getresponse()
            raw = response.read()
            elapsed = (time.perf_counter_ns()-started)/1e6
        except BaseException:
            conn.close()
            self.local.connection = None
            raise
        if response.status not in allowed:
            raise RuntimeError((method, path, response.status, raw[:2000]))
        self.last_status = response.status
        return json.loads(raw), elapsed

    def query(self, query):
        return self.call('POST', '/api/v1/index/bench/query', query)

def digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

def oid(epoch, nonce):
    return {'epoch': epoch, 'nonce': nonce}

def request(server, method, path, body=None):
    """Bounded explicit Busy retries only; same object/IDs, all attempts retained."""
    started = time.perf_counter()
    attempts = []
    known = {}
    for _ in range(1200):
        result, elapsed = server.call(method, path, body, allowed=(200, 429))
        items = result.get('results', [])
        busy_items = sum(row.get('code') == 'busy' for row in items)
        attempts.append({'status': server.last_status, 'http_ms': elapsed, 'response': result, 'busy_items': busy_items})
        for row in items:
            if row.get('receipt'):
                key = digest(row['operation_id'])
                assert key not in known or known[key] == row['receipt'], ('replayed receipt changed', row)
                known[key] = row['receipt']
        code = result.get('code', (result.get('error') or {}).get('code'))
        retryable_items = all(row.get('code') in ('applied', 'committed', 'busy', 'not_attempted') for row in items)
        retryable = (code == 'busy' or busy_items > 0) and retryable_items
        if not retryable:
            return result, (time.perf_counter()-started)*1000, attempts
        if time.perf_counter()-started > 120:
            break
        time.sleep(.01)
    raise RuntimeError(('Busy deadline', attempts))

def files(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob('*')) if p.is_file()}

def stats(v):
 if not v:return {'n':0}
 return {'n':len(v),'mean_ms':statistics.mean(v),**m.quantiles(v),'max_ms':max(v)}

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
