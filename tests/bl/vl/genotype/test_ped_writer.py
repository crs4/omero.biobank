import unittest
import time, os

import numpy as np

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.genotype.io import PedWriter

class ped_writer(unittest.TestCase):

  def setUp(self):
    self.kb = KB(driver='omero')('localhost', 'root', 'romeo')

  def tearDown(self):
    pass

  def test_base(self):
    def extract_data_sample(group, mset, dsample_name):
      by_individual = {}
      for i in self.kb.get_individuals(group):
        gds = filter(lambda x: x.snpMarkersSet == mset,
                     self.kb.get_data_samples(i, dsample_name))
        assert(len(gds) == 1)
        by_individual[i.id] = gds[0]
      return by_individual

    study = self.kb.get_study('TEST01')
    family = self.kb.get_individuals(study)
    mset = self.kb.get_snp_markers_set(label='FakeTaqSet01')
    gds_by_individual = extract_data_sample(study, mset, 'GenotypeDataSample')

    pw = PedWriter(mset, base_path="./foo")
    pw.write_map()
    pw.write_family(study.id, family, gds_by_individual)
    pw.close()


def suite():
  suite = unittest.TestSuite()
  suite.addTest(ped_writer('test_base'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

