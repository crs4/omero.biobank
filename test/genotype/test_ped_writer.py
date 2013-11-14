# BEGIN_COPYRIGHT
# END_COPYRIGHT

# FIXME:
#  1. it assumes to be launched after ../tools/importers/test_gdo_workflow.sh
#  2. no consistency check on written data

import unittest, tempfile, os, time

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.genotype.io import PedWriter

from common import UTCommon

STUDY_LABEL = 'GDO_TEST_STUDY'
MS_LABEL = 'GDO_TEST_MS'
REF_GENOME = 'hg19'

OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'root')
OME_PASSWD = os.getenv('OME_PASSWD', 'romeo')


class TestPedWriter(UTCommon):

    def __init__(self, name):
        super(TestPedWriter, self).__init__(name)
        self.kill_list = []

    def __extract_data_sample(self, group, mset, dsample_name):
        by_individual = {}
        for i in self.kb.get_individuals(group):
            gds = filter(lambda x: x.snpMarkersSet == mset,
                         self.kb.get_data_samples(i, dsample_name))
            assert(len(gds) == 1)
            by_individual[i.id] = gds[0]
        return by_individual

    def setUp(self):
        N_I = 8
        N_M = 12
        self.wd = tempfile.mkdtemp(prefix="biobank_")
        self.kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASSWD)
        conf = {
            'label': 'TEST-%f' % time.time(),
            'description': 'unit test garbage',
            }
        self.study = self.kb.factory.create(self.kb.Study, conf).save()
        self.kill_list.append(self.study)
        self.action = self.kb.create_an_action(self.study)
        self.kill_list.append(self.action)
        mset, rows = self.create_markers_set(N_M)
        self.mset = mset
        self.kill_list.append(self.mset)
        reference_genome = self.create_reference_genome(self.action)
        self.kill_list.append(reference_genome)
        self.vcs = self.create_variant_call_support(self.mset, reference_genome,
                                                    self.action)
        self.kill_list.append(self.vcs)        
        
        for _ in xrange(N_I):
            _, enr = self.create_enrollment(study=self.study)
            self.kill_list.append(enr.save())
            _, action = self.create_action(self.kb.ActionOnIndividual, 
                                        enr.individual)
            self.kill_list.append(action.save())            
            action.reload()
            data_sample = self.create_data_sample(mset, self.make_random_str(),
                                                  action)
            self.kill_list.append(data_sample)
            data_obj, probs, confs = self.create_data_object(data_sample, 
                                                             action)
            self.kill_list.append(data_obj)

    def tearDown(self):
        while self.kill_list:
            self.kb.delete(self.kill_list.pop())

    def test_base(self):
        N_I = 4
        study = self.study        
        base_path = os.path.join(self.wd, "test")
        print "\nwriting to %s*" % base_path
        family = self.kb.get_individuals(study)
        gds_by_individual = self.__extract_data_sample(
            study, self.mset, 'GenotypeDataSample'
        )
        pw = PedWriter(self.vcs, base_path=base_path)
        pw.write_map()
        pw.write_family(study.id, family, gds_by_individual)
        pw.close()
        

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestPedWriter('test_base'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
