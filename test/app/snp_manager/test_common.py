# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, unittest, logging
logging.basicConfig(level=logging.DEBUG)

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
      ((IN_TOP_RC, "A", "C"), (IN_TOP, False)),
      ((IN_TOP_RC, "C", "A"), (IN_TOP, True)),
      ((IN_TOP, "A", "C"), (IN_TOP, False)),
      ((IN_TOP, "C", "A"), (IN_TOP, True)),
      ]

  def runTest(self):
    logger = logging.getLogger()
    sys.stderr.write("\n")
    for args, ret in self.cases:
      self.assertEqual(common.process_mask(*args, logger=logger), ret)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestProcessMask("runTest"))
  return suite


if __name__ == "__main__":
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
