# BEGIN_COPYRIGHT
# END_COPYRIGHT

# FIXME:
#  1. it assumes to be launched after ../tools/importers/test_gdo_workflow.sh
#  2. no consistency check on written data

import unittest, tempfile, os

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.genotype.io import VCFWriter


STUDY_LABEL = 'GDO_TEST_STUDY'
MS_LABEL = 'GDO_TEST_MS'
REF_GENOME = 'hg19'


class TestVCFWriter(unittest.TestCase):

  def setUp(self):
    self.kb = KB(driver='omero')('localhost', 'root', 'romeo')
    self.wd = tempfile.mkdtemp(prefix="biobank_")

  def tearDown(self):
    self.kb.disconnect()

  def __extract_data_sample(self, group, mset, dsample_name):
    dsamples = []
    for i in self.kb.get_individuals(group):
      gds = filter(lambda x: x.snpMarkersSet == mset,
                   self.kb.get_data_samples(i, dsample_name))
      assert(len(gds) == 1)
      dsamples.append(gds[0])
    return dsamples

  def test_base(self):
    base_path = os.path.join(self.wd, "test")
    print "\nwriting to %s*" % base_path
    study = self.kb.get_study(STUDY_LABEL)
    mset = self.kb.get_snp_markers_set(label=MS_LABEL)
    dsamples = self.__extract_data_sample(study, mset, 'GenotypeDataSample')
    vcfw = VCFWriter(mset, ref_genome=REF_GENOME)
    with open('%s.vcf' % base_path, 'w') as fo:
      vcfw.write(fo, dsamples)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestVCFWriter('test_base'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
