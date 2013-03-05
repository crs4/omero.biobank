# BEGIN_COPYRIGHT
# END_COPYRIGHT

# FIXME: it assumes to be launched after ../tools/importers/test_gdo_workflow.sh

import unittest

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.genotype.io import VCFWriter


STUDY_LABEL='GDO_TEST_STUDY'
MS_LABEL='GDO_TEST_MS'
REF_GENOME='hg19'

class vcf_writer(unittest.TestCase):

  def setUp(self):
    self.kb = KB(driver='omero')('localhost', 'root', 'romeo')

  def tearDown(self):
    pass

  def test_base(self):
    def extract_data_sample(group, mset, dsample_name):
      dsamples = []
      for i in self.kb.get_individuals(group):
        gds = filter(lambda x: x.snpMarkersSet == mset,
                     self.kb.get_data_samples(i, dsample_name))
        assert(len(gds) == 1)
        dsamples.append(gds[0])
      return dsamples

    study = self.kb.get_study(STUDY_LABEL)
    family = self.kb.get_individuals(study)
    mset = self.kb.get_snp_markers_set(label=MS_LABEL)
    dsamples = extract_data_sample(study, mset, 'GenotypeDataSample')

    vcfw = VCFWriter(mset, ref_genome=REF_GENOME)
    with open('foo.vcf', 'w') as fo:
      vcfw.write(fo, dsamples)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(vcf_writer('test_base'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
