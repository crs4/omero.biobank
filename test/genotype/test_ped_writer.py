# BEGIN_COPYRIGHT
# END_COPYRIGHT

# FIXME:
#  1. it assumes to be launched after ../tools/importers/test_gdo_workflow.sh
#  2. no consistency check on written data

import unittest, tempfile, os, time

import numpy as np

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
        self.reference_genome = self.create_reference_genome(self.action)
        self.kill_list.append(self.reference_genome)

    def tearDown(self):
        while self.kill_list:
            print 'next to die:', self.kill_list[-1]
            self.kb.delete(self.kill_list.pop())

    def test_base(self):
        N_I = 4
        pos_0 = np.array([
            (1, 201), (1, 202), (1, 203), (2, 101), (2, 102), (2, 103)],
            dtype=self.kb.VariantCallSupport.NODES_DTYPE)
        mset_0, _ = self.create_markers_set_from_stream(len(pos_0))
        self.kill_list.append(mset_0.save())
        vcs_0 = self.create_variant_call_support(mset_0, self.reference_genome,
                                                 self.action)
        self.kill_list.append(vcs_0)
        for _ in xrange(N_I):
            _, enr = self.create_enrollment(study=self.study)
            self.kill_list.append(enr.save())
            _, action = self.create_action(self.kb.ActionOnIndividual, 
                                        enr.individual)
            self.kill_list.append(action.save())            
            action.reload()
            data_sample = self.create_data_sample(mset_0,
                                                  self.make_random_str(),
                                                  action)
            self.kill_list.append(data_sample)
            data_obj, probs, confs = self.create_data_object(data_sample, 
                                                             action)
            self.kill_list.append(data_obj)
        base_path = os.path.join(self.wd, "test")
        print "\nwriting to %s*" % base_path
        family = self.kb.get_individuals(self.study)
        gds_by_individual = self.__extract_data_sample(
            self.study, mset_0, 'GenotypeDataSample'
        )
        pw = PedWriter(vcs_0, base_path=base_path)
        pw.write_map()
        pw.write_family(self.study.id, family, gds_by_individual)
        pw.close()

    def test_multi(self):
        N_I = 4
        N_M_0 = 3
        N_M_1 = 4
        mset_0, _ = self.create_markers_set_from_stream(N_M_0)
        self.kill_list.append(mset_0)
        vcs_0 = self.create_variant_call_support(mset_0, self.reference_genome,
                                                 self.action, chromosome=1)
        self.kill_list.append(vcs_0)
        #--
        mset_1, _ = self.create_markers_set_from_stream(N_M_1)
        self.kill_list.append(mset_1)
        vcs_1 = self.create_variant_call_support(mset_1, self.reference_genome,
                                                 self.action, chromosome=2)
        self.kill_list.append(vcs_1)        

        gds_by_individual = {}
        for _ in xrange(N_I):
            datasets_by_mset = {}
            _, enr = self.create_enrollment(study=self.study)
            self.kill_list.append(enr.save())
            _, action = self.create_action(self.kb.ActionOnIndividual, 
                                        enr.individual)
            self.kill_list.append(action.save())            
            action.reload()
            #--
            data_sample_0 = self.create_data_sample(mset_0,
                                                    self.make_random_str(),
                                                    action)
            self.kill_list.append(data_sample_0)
            data_obj_0, probs, confs = self.create_data_object(data_sample_0, 
                                                               action)
            self.kill_list.append(data_obj_0)
            datasets_by_mset[mset_0.id] = data_sample_0
            #--
            data_sample_1 = self.create_data_sample(mset_1,
                                                    self.make_random_str(),
                                                    action)
            self.kill_list.append(data_sample_1)
            data_obj_1, probs, confs = self.create_data_object(data_sample_1, 
                                                               action)
            self.kill_list.append(data_obj_1)
            datasets_by_mset[mset_1.id] = data_sample_1
            gds_by_individual[enr.individual.id] = datasets_by_mset
            
        base_path = os.path.join(self.wd, "test")
        print "\nwriting to %s*" % base_path
        family = self.kb.get_individuals(self.study)
        vcs = vcs_0.union(vcs_1)
        pw = PedWriter(vcs, base_path=base_path)
        pw.write_map()
        pw.write_family(self.study.id, family, gds_by_individual)
        pw.close()
        
def suite():
  suite = unittest.TestSuite()
  #suite.addTest(TestPedWriter('test_base'))
  suite.addTest(TestPedWriter('test_multi'))  
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
