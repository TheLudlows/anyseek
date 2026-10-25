import unittest
from observation import observe_batch

class ObservationTests(unittest.TestCase):
    def test_unknown_then_durable_only_uses_get_and_preserves_identity(self):
        write={'operation_id':{'epoch':1,'nonce':'original'},'record_id':'record'}
        receipt={'seq':7,'version':1,'record_id':'record'}
        class Engine:
            def __init__(self):self.calls=[];self.lookups=0
            def request(self,method,path,required=False):
                self.calls.append((method,path))
                if '/operation/' in path:
                    self.lookups+=1;return 200,({'receipt':receipt} if self.lookups==2 else None),1,1
                return 200,{'statistics':{'visible_seq':7}},1,1
        e=Engine();result=observe_batch(e,[write],seconds=1,poll_interval=0)
        self.assertTrue(result['all_receipts_observed'])
        self.assertEqual(len(result['attempts']),2)
        self.assertEqual(result['observed_receipts'][0]['receipt'],receipt)
        self.assertTrue(all(method=='GET' for method,_ in e.calls))
        self.assertEqual(result['submitted'],1)

    def test_expired_observation_does_not_retry_or_claim_rejection(self):
        class Engine:
            def request(self,method,path,required=False):
                assert method=='GET' and path=='/api/v1/index/bench'
                return 503,{'code':'recovery_required'},0,0
        result=observe_batch(Engine(),[{'operation_id':{'epoch':1,'nonce':'original'},'record_id':'r'}],seconds=0)
        self.assertFalse(result['all_receipts_observed']);self.assertEqual(result['observed_receipts'],[])

if __name__=='__main__':unittest.main()
