"""Bounded read-only observation after an unknown batch response.

The original response/deadline remains a failure. Receipt discovery does not
rewrite first-pass success counts, and no write or checkpoint is retried here.
"""
import time
from urllib.parse import quote

def observe_batch(engine, writes, seconds=120, poll_interval=1):
    deadline=time.monotonic()+seconds
    found={};attempts=[]
    while time.monotonic()<deadline and len(found)<len(writes):
        for write in writes:
            op=write['operation_id'];key=(op['epoch'],op['nonce'])
            if key in found:continue
            path=f"/api/v1/index/bench/operation/{op['epoch']}/{quote(op['nonce'],safe='')}"
            status,body,ms,_=engine.request('GET',path,required=False)
            attempts.append({'operation_id':op,'status':status,'body':body,'ms':ms})
            if status==200 and isinstance(body,dict) and body.get('receipt'):
                receipt=body['receipt']
                if receipt['record_id']!=write['record_id']:
                    raise RuntimeError('observed receipt identity mismatch')
                found[key]={'operation_id':op,'receipt':receipt}
            if time.monotonic()>=deadline:break
        if len(found)<len(writes) and time.monotonic()<deadline:time.sleep(poll_interval)
    info=engine.request('GET','/api/v1/index/bench',required=False)[:3]
    return {'observation_budget_seconds':seconds,'all_receipts_observed':len(found)==len(writes),
            'submitted':len(writes),'observed_receipts':list(found.values()),
            'attempts':attempts,'info':info,'note':'Missing receipt alone does not prove rejection; no writes/checkpoints resubmitted.'}
