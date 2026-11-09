import json,os,sys,time,subprocess,fcntl,hashlib,platform
from pathlib import Path
R=Path(__file__).resolve().parent
NAME='record::lexical::runtime::live::reuse_tests::lexical_analysis_reuse_measurement'
def run(label,tag):
 binary=R/(label+'-bench')
 processes=subprocess.check_output(['ps','-eo','stat=,comm='],text=True).splitlines()
 assert not any(len(x.split())==2 and not x.split()[0].startswith('Z') and (x.split()[1] in ['cargo','rustc','server'] or x.split()[1].startswith('anyseek')) for x in processes),processes
 cpu=Path('/sys/fs/cgroup/cpu.stat').read_text();began=time.time()
 env={**os.environ,'TOKIO_WORKER_THREADS':'2','RAYON_NUM_THREADS':'2'}
 with (R/(tag+'.log')).open('w') as log:
  result=subprocess.run(['taskset','-c','0,1',str(binary),NAME,'--exact','--ignored','--nocapture','--test-threads=1'],env=env,stdout=log,stderr=subprocess.STDOUT,timeout=180)
 text=(R/(tag+'.log')).read_text();rows=[json.loads(line.split('REUSE_BENCH ',1)[1]) for line in text.splitlines() if 'REUSE_BENCH ' in line]
 evidence={'label':label,'tag':tag,'exit_code':result.returncode,'started':began,'wall_s':time.time()-began,'sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'cpu_stat_before':cpu,'cpu_stat_after':Path('/sys/fs/cgroup/cpu.stat').read_text(),'rows':rows}
 (R/(tag+'.json')).write_text(json.dumps(evidence,indent=2)+'\n')
 assert result.returncode==0 and len(rows)==4,text[-3000:]
 assert all(row['field_calls']==row['records']*(2 if label=='baseline' else 1) for row in rows),rows
 print(tag,[(x['profile'],x['ngram'],round(x['records_s'],2),x['field_calls']) for x in rows],flush=True)
with open('/tmp/anyseek-performance.lock','w') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 if sys.argv[1]=='pilot':run('baseline','baseline-pilot')
 else:
  prefix='confirm' if sys.argv[1]=='confirm' else 'pair'
  (R/('confirmation-environment.json' if prefix=='confirm' else 'measurement-environment.json')).write_text(json.dumps({'platform':platform.platform(),'clock_ticks':os.sysconf('SC_CLK_TCK'),'cpu_max':Path('/sys/fs/cgroup/cpu.max').read_text(),'memory_max':Path('/sys/fs/cgroup/memory.max').read_text(),'cpu_affinity':[0,1],'seed':'fixed repeated text; 24 unique records + 1 warm per profile','source_base':'ed3052f2afdbcf18392a76a4bd3816e4e96275a1','rustc':subprocess.check_output(['rustc','--version'],text=True).strip(),'build':{'profile':'release test','opt_level':3,'lto':False,'codegen_units':16,'features':'default + fault-injection','panic':'test unwind'},'baseline_instrumentation_sha256':hashlib.sha256((R/'baseline-instrumentation.patch').read_bytes()).hexdigest()},indent=2)+'\n')
  for i in range(20 if prefix=='confirm' else 5):
   for label in (['baseline','candidate'] if i%2==0 else ['candidate','baseline']):run(label,f'{prefix}{i}-{label}')
