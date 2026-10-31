import math
import random
import unittest

from gapi.ranking import pagerank, pagerank_sparse


class PackedPageRankTests(unittest.TestCase):
    def test_matches_reference_model_on_asymmetric_dangling_and_random_graphs(self):
        rng=random.Random(9483)
        for n in (2,10,50):
            nodes=[str(i) for i in range(n)]
            edges={(a,b):rng.random()*3 for a in nodes for b in nodes if a!=b and rng.random()<.12}
            edges[(nodes[-1],nodes[0])]=1
            edges[('outside',nodes[0])]=2
            for alpha in (.65,.85,.95):
                expected,_=pagerank(nodes,edges,alpha,1e-12,1000)
                actual,conv=pagerank_sparse(nodes,((a,b,w) for (a,b),w in edges.items()),alpha,1e-12,1000)
                self.assertTrue(conv['converged'])
                self.assertAlmostEqual(math.fsum(actual.values()),1)
                for node in nodes:self.assertAlmostEqual(expected[node],actual[node],places=13)

    def test_direction_and_boundary_mass(self):
        result,_=pagerank_sparse(['X','Y'],[('Y','X',1),('Y','outside',100)])
        self.assertGreater(result['X'],result['Y'])
        self.assertAlmostEqual(sum(result.values()),1)

    def test_invalid_inputs_and_resource_cancellation(self):
        for edge in [('X','Y',-1),('X','Y',float('nan'))]:
            with self.assertRaises(ValueError):pagerank_sparse(['X','Y'],[edge])
        with self.assertRaises(ValueError):pagerank_sparse(['X','X'],[])
        def check():raise RuntimeError('resource boundary')
        with self.assertRaisesRegex(RuntimeError,'resource boundary'):
            pagerank_sparse(['X','Y'],[('X','Y',1)],check=check)
