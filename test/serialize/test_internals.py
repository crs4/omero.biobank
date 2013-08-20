# BEGIN_COPYRIGHT
# END_COPYRIGHT

import bl.vl.app.loader.util as util
import bl.vl.app.loader.deserialize as deserialize

from pygraph.classes.digraph import digraph

import sys, unittest, itertools as it
import numpy as np

class TestInternals(unittest.TestCase):
    def setUp(self):
        pass

    def test_sort_by_dependency(self):
        g = digraph()
        M, N = 10, 500
        g.add_nodes(map(str, range(M*N)))
        for l in range(M - 1):
            for k in range(N):
                lower_node, upper_node = str(l*N + k), str((l+1)*N + k)
                g.add_node_attribute(lower_node, ('level', M - l))
                g.add_node_attribute(upper_node, ('level', M - (l + 1)))
                g.add_edge((upper_node, lower_node))
        ordered = util.sort_by_dependency(g)
        level = 0
        for n in ordered:
            new_level = dict(g.node_attr[n])['level']
            self.assertTrue(new_level >= level)
            level = new_level

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestInternals('test_sort_by_dependency'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
