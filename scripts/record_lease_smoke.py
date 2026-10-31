"""Private staging leases, using only temporary fixture roots and test clocks."""
import http.client
import os
import time
from record_service_smoke import operation


def exercise(call,restart_with,directory,faults,only_faults=False,*,tenant_key):
    now=int(time.time()*1000);clock_file=directory/'lease-clock-ms'
    def clock(value):
        pending=clock_file.with_suffix('.next');pending.write_text(str(value));os.replace(pending,clock_file)
    clock(now);env={'ANYSEEK_TEST_CLOCK_FILE':str(clock_file)} if faults else {}
    restart_with(env)
    config={'index_name':'Leases','schema':[{'field':'body','field_type':'Text','store':True}]}
    directories={}
    def create(cid):
        endpoint='/api/v1/index/'+cid
        oid=operation()
        call('POST','/api/v1/index',{'operation_id':oid,'collection_id':cid,'config':config})
        seq=call('GET',f"/api/v1/catalog/operation/{oid['epoch']}/{oid['nonce']}",key=tenant_key)['receipt']['seq']
        directories[endpoint]=f'collections/test/{cid}/{seq:016x}'
        call('POST',endpoint+'/doc',{'operation_id':operation(),'record_id':'s','expected':'Absent','document':{'body':'source'},'system':{'kind':'source'}})
        return endpoint
    def stage(g='g',expected='Absent'):
        return {'operation_id':operation(),'record_id':'doc','expected':expected,'generation':{'action':'stage','generation_id':g,'source':{'record_id':'s','version':1},'members':['chunk-'+g],'chunks':[{'record_id':'chunk-'+g,'document':{'body':'PRIVATE-LEASE-PAYLOAD-'+g}}]}}
    def command(action,version,g='g'):
        return {'operation_id':operation(),'record_id':'doc','expected':{'Match':version},'generation':{'action':action,'generation_id':g}}
    if not only_faults:
        endpoint=create('leases');original=stage();receipt=call('POST',endpoint+'/doc',original)['receipt']
        status=call('GET',endpoint+'/doc/doc/generation');assert status['lease_fence']==1 and status['staged_members']==1
        renewal=command('renew',1);renewed=call('POST',endpoint+'/doc',renewal)['receipt'];assert renewed['staged_only']
        assert call('POST',endpoint+'/doc',renewal)['receipt']==renewed
        call('POST',endpoint+'/doc',command('publish',1),status=409)
        restart_with(env);assert call('POST',endpoint+'/doc',original)['receipt']==receipt
    if faults:
        endpoint=create('lease-cleanup');request=stage();receipt=call('POST',endpoint+'/doc',request)['receipt']
        restart_with(env)
        # Arm only this collection after recovery. A global cleanup fault would
        # correctly fence S3 cold startup while it materializes old checkpoints,
        # before the private-abort path under test can run.
        cache=directory/'data'/directories[endpoint]
        cleanup_fault=cache/'.test-cleanup-failure'
        cleanup_fault.write_text('fail this collection old-generation retirement')
        call('POST',endpoint+'/doc',command('abort',1))
        assert call('GET',endpoint+'/doc/doc/generation')['private_cleanup_pending']
        assert call('POST',endpoint+'/doc',request)['receipt']==receipt
        cleanup_fault.unlink()
        restart_with(env)
        assert not call('GET',endpoint+'/doc/doc/generation')['private_cleanup_pending']
        assert call('POST',endpoint+'/doc',request)['receipt']==receipt
        for fault in ['after_append_unknown','after_durable','crash_after_durable','crash_after_visible']:
            clock(now);endpoint=create('lease-'+fault.replace('_','-'));request=stage();receipt=call('POST',endpoint+'/doc',request)['receipt']
            restart_with({**env,'ANYSEEK_LEASE_FAULT':fault});clock(now+1_800_000)
            if fault.startswith('crash_'):
                try:call('GET',endpoint+'/doc/doc/generation')
                except (OSError,http.client.HTTPException):pass
                else:raise AssertionError('expected lease process exit')
            else:assert call('GET',endpoint+'/doc/doc/generation',status=503)['code'] in ('outcome_unknown','durable_pending')
            restart_with(env);status=call('GET',endpoint+'/doc/doc/generation');assert status['staged_members']==0 and status['version']==2 and not status['private_cleanup_pending'], (fault, status)
            assert call('POST',endpoint+'/doc',request)['receipt']==receipt
            call('POST',endpoint+'/doc',command('renew',1),status=409)
            assert call('GET',endpoint+'/doc/doc/generation')['visible_members']==0
    restart_with({})
    print('PASS staging leases HTTP: fencing, renewal/retry, private abort cleanup and retained receipts'+(' with controlled expiry/process faults' if faults else ''))
