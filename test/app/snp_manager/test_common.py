# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, unittest
import bl.vl.app.snp_manager.common as common


NOT_CONVERTIBLE = "CAAA[C/G]AATG"
IN_TOP = "GTAT[A/C]AAAA"
IN_TOP_RC = 'TTTT[G/T]ATAC'


class TestProcessMask(unittest.TestCase):

  def setUp(self):
    self.cases = [
      (("XYZ", "A", "C"), ("None", False)),
      (("ACTG[A/C/T]GTGA", "A", "C"), ("None", False)),
      (("ACTG[A/Z]GTGA", "A", "Z"), ("None", False)),
      ((NOT_CONVERTIBLE, "C", "G"), (NOT_CONVERTIBLE, False)),
      ((NOT_CONVERTIBLE, "G", "C"), (NOT_CONVERTIBLE, True)),
      ((IN_TOP, "A", "C"), (IN_TOP, False)),
      ((IN_TOP, "C", "A"), (IN_TOP, True)),
      ((IN_TOP_RC, "G", "T"), (IN_TOP, False)),
      ((IN_TOP_RC, "T", "G"), (IN_TOP, True)),
      ]

  def runTest(self):
    sys.stderr.write("\n")
    for args, (exp_mask, exp_allele_flip) in self.cases:
      mask, allele_flip, error = common.process_mask(*args)
      self.assertEqual(mask, exp_mask)
      self.assertEqual(allele_flip, exp_allele_flip)
      sys.stderr.write("%s\n" % (error or "(no error)"))


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestProcessMask("runTest"))
  return suite


if __name__ == "__main__":
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
