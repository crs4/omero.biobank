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
    self.assertFalse(wf.is_modified)
    wf.annotation = 'a new annotation'
    self.assertTrue(wf.is_modified)

  def test_workflow_steps(self):
    with open('Galaxy-Workflow-Hepatocyte.ga') as f:
      wk_desc = json.load(f)
    wf = cwps.Workflow(wk_desc)
    for s in wf.steps:
      s_desc = wk_desc['steps'][str(s.id)]
      for k in s_desc:
        if k in ['tool_errors', 'tool_id', 'tool_version']:
          if s.type == 'tool':
            self.assertEqual(getattr(s.tool, k.replace('tool_', '')), s_desc[k])
          continue
        self.assertEqual(getattr(s, k), s_desc[k])
    self.assertFalse(wf.is_modified)

  def test_workflow_step_taint(self):
    with open('Galaxy-Workflow-Hepatocyte.ga') as f:
      wk_desc = json.load(f)
    wf = cwps.Workflow(wk_desc)
    self.assertFalse(wf.is_modified)
    wf.steps[0].annotation = 'a new annotation'
    self.assertTrue(wf.is_modified)

  def test_workflow_tool_taint(self):
    with open('Galaxy-Workflow-Hepatocyte.ga') as f:
      wk_desc = json.load(f)
    wf = cwps.Workflow(wk_desc)
    self.assertFalse(wf.is_modified)
    wf.steps[4].tool['format'] = 'a new format'
    self.assertTrue(wf.is_modified)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestCoreWrappers('test_wrapper_initialize'))
  suite.addTest(TestCoreWrappers('test_wrapper_taint'))
  suite.addTest(TestCoreWrappers('test_workflow_initialize'))
  suite.addTest(TestCoreWrappers('test_workflow_steps'))
  suite.addTest(TestCoreWrappers('test_workflow_step_taint'))
  suite.addTest(TestCoreWrappers('test_workflow_tool_taint'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
