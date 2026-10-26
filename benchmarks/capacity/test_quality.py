import math
import unittest
from quality import evaluate

class QualityMetrics(unittest.TestCase):
    def fixtures(self):
        return [dict(qid='a',group='prior40_nonblind',qrels={'a':2,'b':1}),dict(qid='b',group='heldout353',qrels={'c':1})]
    def test_graded_rank_and_errors_keep_all_queries_in_denominator(self):
        values=evaluate(self.fixtures(),[dict(status=200,ids=['b','a']),dict(status=503,ids=[])],{'a','b','c'})
        self.assertEqual(values['all']['recall10'],.5)
        self.assertEqual(values['all']['recall100'],.5)
        self.assertEqual(values['all']['mrr10'],.5)
        expected=(1+3/math.log2(3))/(3+1/math.log2(3))
        self.assertAlmostEqual(values['all']['ndcg10'],expected/2)
        self.assertEqual(values['all']['error_rate'],.5)
        self.assertEqual(values['all']['empty_over_all_queries'],0)
        self.assertEqual(values['heldout353']['errors'],1)
    def test_empty_is_distinct_from_error_and_missing_positive_not_removed(self):
        values=evaluate(self.fixtures(),[dict(status=200,ids=['a']),dict(status=200,ids=[])],{'a','c'})
        self.assertEqual(values['prior40_nonblind']['recall10'],.5)
        self.assertEqual(values['prior40_nonblind']['positive_pairs'],2)
        self.assertEqual(values['prior40_nonblind']['covered_positive_pairs'],1)
        self.assertEqual(values['all']['error_rate'],0)
        self.assertEqual(values['all']['empty_over_all_queries'],.5)
    def test_duplicate_or_outside_pool_hits_fail_validation(self):
        for ids in [['a','a'],['outside']]:
            with self.assertRaises(AssertionError):
                evaluate(self.fixtures(),[dict(status=200,ids=ids),dict(status=200,ids=[])],{'a','b','c'})

if __name__=='__main__':unittest.main()
