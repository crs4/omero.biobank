# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, time
import numpy as np

import bl.vl.utils.np_ext as np_ext


class TestIndexIntersect(unittest.TestCase):

  def test_simple_array(self):
    a1 = np.array(list('axzbykcq'))
    a2 = np.array(list('12ab34'))
    i1, i2 = np_ext.index_intersect(a1, a2)
    self.assertEqual(i1.tolist(), [0, 3])
    self.assertEqual(i2.tolist(), [2, 3])
    i3, i4 = np_ext.index_intersect(a2, a1)
    self.assertTrue(np.array_equal(i4, i1))
    self.assertTrue(np.array_equal(i3, i2))

  def test_record_array(self):
    a1 = np.array([('V002', 0), ('V068', 1), ('V129', 0)],
                  dtype=[('marker_vid', '|S34'), ('allele_flip', '|i1')])
    a2 = np.array([('V%03d' % i, 'foo-%03d' % i) for i in xrange(1000)],
                  dtype=[('vid', '|S34'), ('label', '|S48')])
    i1, i2 = np_ext.index_intersect(a1['marker_vid'], a2['vid'])
    self.assertEqual(i1.tolist(), [0, 1, 2])
    self.assertEqual(i2.tolist(), [2, 68, 129])
    self.assertEqual(a1[i1].tolist(), [('V002', 0), ('V068', 1), ('V129', 0)])
    self.assertEqual(
      a2[i2].tolist(),
      [('V002', 'foo-002'), ('V068', 'foo-068'), ('V129', 'foo-129')]
      )

  def test_performance(self):
    size = 1000000
    offset = size/10000
    a1 = np.array(['V%07d' % i for i in xrange(offset, size)])
    a2 = np.array(['V%07d' % i for i in xrange(offset+size)])
    t0 = time.time()
    i1, i2 = np_ext.index_intersect(a1, a2)
    print
    print "finished in %.1f s" % (time.time()-t0)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestIndexIntersect('test_simple_array'))
  suite.addTest(TestIndexIntersect('test_record_array'))
  #suite.addTest(TestIndexIntersect('test_performance'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
