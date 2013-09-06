# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, unittest, logging, shutil
logging.basicConfig(level=logging.ERROR)

import bl.vl.kb.galaxy.core_wrappers as cwps
import json

class TestCoreWrappers(unittest.TestCase):
  def test_wrapper_initialize(self):
    d = {'a' : 1, 'b' : 2,  'c': 3}
    w = cwps.Wrapper(d)
    for k in d:
      self.assertEqual(getattr(w, k), d[k])
    w.a = 222
    self.assertTrue(w.a, 222)

  def test_wrapper_taint(self):
    d = {'a' : 1, 'b' : 2,  'c': 3}
    w = cwps.Wrapper(d)
    self.assertFalse(w.is_modified)
    w.a = 111
    self.assertTrue(w.is_modified)

  def test_workflow_initialize(self):
    with open('Galaxy-Workflow-Hepatocyte.ga') as f:
      wk_desc = json.load(f)
    wf = cwps.Workflow(wk_desc)
    for k in wk_desc:
      if k in ['steps']:
        continue
      self.assertEqual(getattr(wf, k), wk_desc[k])
        
def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestCoreWrappers('test_wrapper_initialize'))
  suite.addTest(TestCoreWrappers('test_wrapper_taint'))  
  suite.addTest(TestCoreWrappers('test_workflow_initialize'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
