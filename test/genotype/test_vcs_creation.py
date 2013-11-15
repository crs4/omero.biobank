# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, time, os, random
import itertools as it
import tempfile
import numpy as np

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.kb.drivers.omero.genomics import MSET_TABLE_COLS_DTYPE

from common import UTCommon

import bl.vl.genotype.io as gio

OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'root')
OME_PASSWD = os.getenv('OME_PASSWD', 'romeo')

class vcs_creation(UTCommon):

  def __init__(self, name):
    super(vcs_creation, self).__init__(name)
    self.kill_list = []

  def setUp(self):
    self.kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASSWD)
    conf = {
      'label': 'TEST-%f' % time.time(),
      'description': 'unit test garbage',
      }
    self.study = self.kb.factory.create(self.kb.Study, conf).save()
    self.kill_list.append(self.study)
    self.action = self.kb.create_an_action(self.study)
    self.kill_list.append(self.action)

  def tearDown(self):
    while self.kill_list:
      self.kb.delete(self.kill_list.pop())

  def test_creation(self):
    positions = np.array([
      (1, 201), (1, 202), (2, 203), (1, 204), (1, 201), (2, 202), (1, 203), 
      (2, 201), (1, 202), (2, 203), (1, 204), (1, 201), (1, 202), (2, 203)],
      dtype=self.kb.VariantCallSupport.NODES_DTYPE)
    good_nodes = np.array([
      (1, 201), (1, 202), (1, 203), (1, 204), 
      (2, 201), (2, 202), (2, 203) ],
      dtype=self.kb.VariantCallSupport.NODES_DTYPE)
    duplicates = np.array([[0, 1, 3, 6], 
                           [3, 3, 2, 3]], dtype=np.int32)
    N = len(positions)
    mset, rows = self.create_markers_set(N)
    self.kill_list.append(mset)
    ref_genome = self.create_reference_genome(self.action)
    self.kill_list.append(ref_genome)    
    vcs = self.kb.genomics.create_vcs(mset, ref_genome, positions, self.action)
    origin = vcs.get_field('origin')
    self.assertTrue((good_nodes == vcs.get_nodes()).all())
    self.assertEqual(len(origin), len(positions))
    counts = vcs.get_multiple_origins_nodes()
    self.assertTrue((duplicates ==counts).all())

def suite():
  suite = unittest.TestSuite()
  suite.addTest(vcs_creation('test_creation'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
