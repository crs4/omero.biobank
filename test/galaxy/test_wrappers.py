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
    self.assertFalse(wf2 == wf)
    self.assertEqual(wf2.id, None)
    self.assertEqual(wf.to_json(), wf2.to_json())

  def test_workflow_inputs(self):
    with open('Galaxy-Workflow-Hepatocyte.ga') as f:
      wf_desc = json.load(f)
    wf_id = '89898989898'
    wf_ports = {
      "inputs": {"contigs" :
                 {"type" : "DataCollection",
                  "fields":   {"contigs":
                               {"port" : {"step" : "0", "name": "contigs"},
                                "mimetype" : "x-vl/fasta"},
                               "reads":
                               {"port" : {"step" : "1", "name": "reads"},
                                "mimetype" : "x-vl/fasta"},
                               "mates":
                               {"port" : {"step" : "2", "name": "mates"},
                                "mimetype" : "x-vl/fasta"}}}},
      "outputs": {"scaffolding":
                  {"type" : "DataCollection",
                   "fields" : {"finalevidence":
                                 {"port": {"step":"3","name" : "finalevidence"},
                                  "mimetype" : "text/plain"},
                               "summary":
                                 {"port": {"step" : "3", "name": "summary"},
                                  "mimetype" : "text/plain"}}}}
               }
    wf_links = {'71': {'label' : 'contigs', 'value': ''},
                '72': {'label' : 'reads', 'value': ''},
                '73': {'label' : 'mates', 'value': ''}}
    links_by_label = dict([(v['label'], k) for k, v in wf_links.iteritems()])
    wf = glxy.Workflow(wf_id, wf_desc, wf_ports, wf_links)
    self.assertEqual(set(wf.ports.keys()), set(wf_ports.keys()))
    for k in wf.ports:
      self.assertEqual(wf.ports[k], wf_ports[k])
    self.assertEqual(set(wf.links.keys()), set(links_by_label.keys()))
    for k in wf.links:
      self.assertEqual(wf.links[k], links_by_label[k])


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestWrappers('test_workflow_init'))
  suite.addTest(TestWrappers('test_workflow_clone'))
  suite.addTest(TestWrappers('test_workflow_inputs'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
