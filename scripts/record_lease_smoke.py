"""Private staging leases, using only temporary fixture roots and test clocks."""
import http.client
import os
import time
from record_service_smoke import operation


def exercise(call,restart_with,directory,faults,only_faults=False):
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
        directories[endpoint]=call('GET',f"/api/v1/catalog/operation/{oid['epoch']}/{oid['nonce']}")['receipt']['directory']
        call('POST',endpoint+'/doc',{'operation_id':operation(),'record_id':'s','routing_key':'r','expected':'Absent','document':{'body':'source'},'system':{'kind':'source'}})
        return endpoint
    def stage(g='g',expected='Absent'):
        return {'operation_id':operation(),'record_id':'doc','routing_key':'r','expected':expected,'generation':{'action':'stage','generation_id':g,'source':{'record_id':'s','routing_key':'r','version':1},'members':['chunk-'+g],'chunks':[{'record_id':'chunk-'+g,'document':{'body':'PRIVATE-LEASE-PAYLOAD-'+g}}]}}
    def command(action,version,g='g'):
        return {'operation_id':operation(),'record_id':'doc','routing_key':'r','expected':{'Match':version},'generation':{'action':action,'generation_id':g}}
    if not only_faults:
        endpoint=create('leases');original=stage();receipt=call('POST',endpoint+'/doc',original)['receipt']
        status=call('GET',endpoint+'/doc/doc/generation');assert status['lease_fence']==1 and status['staged_members']==1
        renewal=command('renew',1);renewed=call('POST',endpoint+'/doc',renewal)['receipt'];assert renewed['staged_only']
        assert call('POST',endpoint+'/doc',renewal)['receipt']==renewed
        call('POST',endpoint+'/doc',command('publish',1),status=409)
        archive=call('POST',endpoint+'/backup',{'operation_id':operation()})
        take={'operation_id':operation(),'source_collection_id':'leases','collection_id':'leases-next','backup_id':archive['backup_id']}
        assert call('POST','/api/v1/takeovers',take)['status']=='active';endpoint='/api/v1/index/leases-next'
        assert call('GET',endpoint+'/doc/doc/generation')['lease_requires_renewal']
        call('POST',endpoint+'/doc',command('publish',2),status=409)
        call('POST',endpoint+'/doc',command('renew',2))
        call('POST',endpoint+'/doc',command('abort',3))
        assert call('POST',endpoint+'/doc',original)['receipt']==receipt
        assert call('GET',endpoint+'/doc/doc/generation')['staged_members']==0
        call('POST',endpoint+'/doc',stage('g',{'Match':4}),status=409)
        call('DELETE','/api/v1/backups/'+archive['backup_id'],{'operation_id':operation()})
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
            assert [e['kind'] for e in call('POST',endpoint+'/changes',{})['events']]==['source_created']
    restart_with({})
    print('PASS staging leases HTTP: fencing, renewal/retry, takeover requires renewal, private abort cleanup and retained receipts'+(' with controlled expiry/process faults' if faults else ''))
