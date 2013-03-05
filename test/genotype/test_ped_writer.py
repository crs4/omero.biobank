# BEGIN_COPYRIGHT
# END_COPYRIGHT

# FIXME: it assumes to be launched after ../tools/importers/test_gdo_workflow.sh

import unittest

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.genotype.io import PedWriter

STUDY_LABEL='GDO_TEST_STUDY'
MS_LABEL='GDO_TEST_MS'
REF_GENOME='hg19'


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

    study = self.kb.get_study(STUDY_LABEL)
    family = self.kb.get_individuals(study)
    mset = self.kb.get_snp_markers_set(label=MS_LABEL)
    gds_by_individual = extract_data_sample(study, mset, 'GenotypeDataSample')

    pw = PedWriter(mset, base_path="./foo", ref_genome=REF_GENOME)
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
