# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, unittest, logging, shutil
logging.basicConfig(level=logging.ERROR)

import bl.vl.kb.galaxy as glxy
import json

class TestWrappers(unittest.TestCase):

  def test_workflow_init(self):
    with open('Galaxy-Workflow-Hepatocyte.ga') as f: 
      wf_desc = json.load(f)
    wf_id = '89898989898'
    wf = glxy.Workflow(wf_id, wf_desc)
    self.assertEqual(wf.id, wf_id)

  def test_workflow_clone(self):
    with open('Galaxy-Workflow-Hepatocyte.ga') as f: 
      wf_desc = json.load(f)
    wf_id = '89898989898'
    wf = glxy.Workflow(wf_id, wf_desc)
    self.assertEqual(wf.id, wf_id)
    wf2 = wf.clone()
    self.assertEqual(wf2.id, None)    
    self.assertEqual(wf.to_json(), wf2.to_json())
    
def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestWrappers('test_workflow_init'))
  suite.addTest(TestWrappers('test_workflow_clone'))  
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
    
