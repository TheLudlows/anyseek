import json,resource,subprocess,sys
from pathlib import Path
p=subprocess.run(sys.argv[2:])
r=resource.getrusage(resource.RUSAGE_CHILDREN)
Path(sys.argv[1]).write_text(json.dumps({'max_rss_kib':r.ru_maxrss,'user_s':r.ru_utime,'system_s':r.ru_stime,'status':p.returncode}))
sys.exit(p.returncode)
