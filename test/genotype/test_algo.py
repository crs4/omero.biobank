# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, unittest, itertools as it
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


class TestCountHomozigotes(unittest.TestCase):

  def setUp(self):
    self.prob_list = [
      [[1, 1, 0, 0, 1],
       [0, 0, 0, 0, 0]],
      [[1, 0, 0, 0, 0],
       [0, 1, 1, 1, 1]],
      [[1, 1, 0, 1, 0],
       [0, 0, 1, 0, 0]],
      [[0, 0, 1, 1, 0],
       [0, 0, 0, 0, 1]],
      ]
    self.gdo_stream = (
      dict(probs=np.array(p, dtype=np.float32)) for p in self.prob_list
      )

  def test_no_threshold(self):
    exp_counts = [
      [3, 2, 1, 2, 1],
      [0, 1, 2, 1, 2],
      ]
    n_gdos, counts = algo.count_homozygotes(self.gdo_stream)
    self.assertEqual(n_gdos, len(self.prob_list))
    for c, exp_c in it.izip(counts, exp_counts):
      self.assertEqual(c.tolist(), exp_c)


class TestGenerateData(unittest.TestCase):

  SIZE = 10000
  N_ITER = 100

  def runTest(self):
    params = {"conf_sigma": 0.1, "conf_to_ratio": 1.0}
    size = self.SIZE
    for i in xrange(self.N_ITER):
      if (i + 1) % (self.N_ITER // 10) == 0:
        sys.stderr.write(".")
        sys.stderr.flush()
      probs, confs = algo.generate_data(size, **params)
      symmetric_confs = np.concatenate((-confs, confs))
      self.assertAlmostEqual(
        np.std(symmetric_confs, ddof=1), params["conf_sigma"], 1
        )
      allprobs = np.vstack((probs, 1.0 - probs.sum(axis=0)))
      allprobs.sort(axis=0)
      ratios = allprobs[1] / allprobs[2]
      self.assertTrue(np.allclose(
        ratios, params["conf_to_ratio"] * confs, atol=1e-6
        ))
    sys.stderr.write(" ")


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestProjectToDiscreteGenotype('test_no_threshold'))
  suite.addTest(TestProjectToDiscreteGenotype('test_threshold'))
  suite.addTest(TestCountHomozigotes('test_no_threshold'))
  suite.addTest(TestGenerateData('runTest'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
