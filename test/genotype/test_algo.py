# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest
import numpy as np

import bl.vl.genotype.algo as algo


class TestProjectToDiscreteGenotype(unittest.TestCase):

  def setUp(self):
    self.probs = np.array([
      [1e-4, 0.55, 1e-3, 0.91],
      [0.93, 0.41, 1e-5, 1e-4],
      ])

  def __check_gt(self, threshold, expected_gt):
    gt = algo.project_to_discrete_genotype(self.probs, threshold=threshold)
    self.assertTrue(np.array_equal(gt, expected_gt))

  def test_no_threshold(self):
    self.__check_gt(1.0, [1, 0, 2, 0])

  def test_threshold(self):
    self.__check_gt(0.1, [1, 3, 2, 0])


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestProjectToDiscreteGenotype('test_no_threshold'))
  suite.addTest(TestProjectToDiscreteGenotype('test_threshold'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
