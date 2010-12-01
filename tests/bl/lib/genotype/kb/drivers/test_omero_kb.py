import unittest
import bl.lib.genotype.kb.drivers.omero_kb as kb
import vl.lib.utils as vl_utils


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


def suite():
  suite = unittest.TestSuite()  
  suite.addTest(TestStudy('runTest'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
