# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, unittest, logging, shutil
logging.basicConfig(level=logging.ERROR)

import bl.vl.kb.galaxy.core_wrappers as cwps

class TestCoreWrappers(unittest.TestCase):
  def test_initialize(self):
    d = {'a' : 1, 'b' : 2,  'c': 3}
    w = cwps.Wrapper(d)
    for k in d:
      self.assertEqual(getattr(w, k), d[k])
    w.a = 222
    self.assertTrue(w.a, 222)

  def test_taint(self):
    d = {'a' : 1, 'b' : 2,  'c': 3}
    w = cwps.Wrapper(d)
    self.assertFalse(w.is_modified)
    w.a = 111
    self.assertTrue(w.is_modified)
    
        
def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestCoreWrappers('test_initialize'))
  suite.addTest(TestCoreWrappers('test_taint'))  
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
