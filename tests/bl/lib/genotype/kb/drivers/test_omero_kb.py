import os, unittest
from bl.lib.genotype.kb import KBError
import bl.lib.genotype.kb.drivers.omero_kb as kb
import vl.lib.utils as vl_utils


OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "omero")


class TestStudy(unittest.TestCase):

  def setUp(self):
    self.label = "FOO"
    
  def runTest(self):
    s1 = kb.Study()
    s2 = kb.Study(self.label)
    for s in s1, s2:
      self.assertTrue(hasattr(s, "ome_obj"))
      self.assertTrue(hasattr(s, "id"))
      self.assertTrue(hasattr(s, "label"))
      self.assertEqual(s.id[0], vl_utils.DEFAULT_PREFIX)
      self.assertEqual(s.id[1], vl_utils.DEFAULT_DIGIT)
    self.assertTrue(s1.label is None)
    s1.label = self.label
    for s in s1, s2:
      self.assertEqual(s.label, self.label)


class TestProxy(unittest.TestCase):

  def setUp(self):
    self.proxy = kb.Proxy(OME_HOST, OME_USER, OME_PASS)
    self.study = kb.Study("FOO")

  def test_get_study_by_label(self):
    s = self.proxy.get_study_by_label(self.study.label)

  def test_save_study(self):
    self.assertRaises(KBError, self.proxy.save_study, kb.Study())
    s = self.proxy.save_study(self.study)
    self.proxy.delete(s)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestStudy('runTest'))
  #suite.addTest(TestProxy('test_get_study_by_label'))
  #suite.addTest(TestProxy('test_save_study'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
