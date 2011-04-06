import os, unittest, time
import itertools as it
from bl.lib.individual.kb import KnowledgeBase as iKB
from bl.lib.sample.kb     import KnowledgeBase as sKB

from skb_object_creator import SKBObjectCreator

import logging
logging.basicConfig(level=logging.DEBUG)


OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")

class TestIKB(SKBObjectCreator, unittest.TestCase):
  def __init__(self, name):
    super(TestIKB, self).__init__(name)
    self.kill_list = []

  def setUp(self):
    self.ikb = iKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)
    self.skb = sKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)
    self.atype_map   = self.skb.get_action_type_table()
    self.outcome_map = self.skb.get_result_outcome_table()
    self.sstatus_map = self.skb.get_sample_status_table()
    self.dtype_map   = self.skb.get_data_type_table()

  def tearDown(self):
    self.kill_list.reverse()
    for x in self.kill_list:
      self.skb.delete(x)
    self.kill_list = []

  def check_object(self, o, conf, otype):
    try:
      self.assertTrue(isinstance(o, otype))
      for k in conf.keys():
        v = conf[k]
        if hasattr(v, 'ome_obj'):
          self.assertEqual(getattr(o, k).id, v.id)
          self.assertEqual(type(getattr(o, k)), type(v))
        elif hasattr(v, '_id'):
          self.assertEqual(getattr(o, k)._id, v._id)
        else:
          self.assertEqual(getattr(o, k), v)
    except:
      pass

  def create_individual(self, gender='MALE'):
    gmap = self.ikb.get_gender_table()
    conf = {'gender' : gmap[gender]}
    i = self.ikb.Individual(gender=conf['gender'])
    return conf, i

  def create_enrollment(self):
    conf, study = self.create_study()
    study = self.skb.save(study)
    self.kill_list.append(study)
    #-
    conf, i = self.create_individual('MALE')
    i = self.ikb.save(i)
    self.kill_list.append(i)
    #-
    conf = {'study' : study, 'individual' : i,
            'studyCode' : 'study-code-%f' % time.time()}
    e = self.ikb.Enrollment(study=conf['study'],
                            individual=conf['individual'],
                            study_code=conf['studyCode'])
    return conf, e

  def create_action_on_individual(self):
    conf, individual = self.create_individual()
    individual = self.ikb.save(individual)
    self.kill_list.append(individual)
    #--
    conf, action = self.create_action(action=self.ikb.ActionOnIndividual())
    sconf = { 'target' : individual}
    self.configure_object(action, sconf)
    conf.update(sconf)
    return conf, action

  def test_orphan(self):
    conf, i = self.create_individual('MALE')
    i = self.ikb.save(i)
    self.check_object(i, conf, self.ikb.Individual)
    self.ikb.delete(i)

  def test_with_parents(self):
    conf, f = self.create_individual('MALE')
    f = self.ikb.save(f)
    self.kill_list.append(f)
    #-
    conf, m = self.create_individual('FEMALE')
    m = self.ikb.save(m)
    self.kill_list.append(m)
    #--
    conf, i = self.create_individual('MALE')
    sconf = {'father' : f, 'mother' : m}
    self.configure_object(i, sconf)
    conf.update(sconf)
    i = self.ikb.save(i)
    self.check_object(i, conf, self.ikb.Individual)
    self.ikb.delete(i)

  def test_enrollment(self):
    conf, e = self.create_enrollment()
    e = self.ikb.save(e)
    self.check_object(e, conf, self.ikb.Enrollment)
    self.ikb.delete(e)

  def test_action_on_individual(self):
    conf, action = self.create_action_on_individual()
    action = self.ikb.save(action)
    self.check_object(action, conf, self.ikb.ActionOnIndividual)
    self.ikb.delete(action)

  def create_individual_blood_chain(self):
    conf, action = self.create_action_on_individual()
    action = self.ikb.save(action)
    self.kill_list.append(action)
    #--
    conf, sample = self.create_blood_sample()
    sample.action = action
    return action.target, sample

  def create_individual_blood_dna_chain(self):
    individual, blood_sample = self.create_individual_blood_chain()
    blood_sample = self.skb.save(blood_sample)
    self.kill_list.append(blood_sample)
    #--
    conf, action = self.create_action_on_sample()
    action.target = blood_sample
    action = self.skb.save(action)
    self.kill_list.append(action)
    #--
    conf, dna_sample = self.create_dna_sample()
    dna_sample.action = action
    #--
    return individual, dna_sample

  def test_get_blood_sample(self):
    individual, blood_sample = self.create_individual_blood_chain()
    blood_sample = self.skb.save(blood_sample)
    self.kill_list.append(blood_sample)
    #-
    bs = self.ikb.get_blood_sample(individual)
    self.assertTrue(not bs is None)
    self.assertEqual(bs.id, blood_sample.id)

  def test_get_dna_sample(self):
    individual, dna_sample = self.create_individual_blood_dna_chain()
    dna_sample = self.skb.save(dna_sample)
    self.kill_list.append(dna_sample)
    #-
    dnas = self.ikb.get_dna_sample(individual)
    self.assertTrue(not dnas is None)
    self.assertEqual(dnas.id, dna_sample.id)

  def test_plate_well_dna(self):
    individual, dna_sample = self.create_individual_blood_dna_chain()
    dna_sample = self.skb.save(dna_sample)
    self.kill_list.append(dna_sample)
    #-
    dnas = self.ikb.get_dna_sample(individual)
    #
    conf, plate_well = self.create_plate_well(sample=dna_sample)
    plate_well = self.skb.save(plate_well)
    self.check_object(plate_well, conf, self.skb.PlateWell)
    self.kill_list.append(plate_well)

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestIKB('test_orphan'))
  suite.addTest(TestIKB('test_with_parents'))
  suite.addTest(TestIKB('test_enrollment'))
  suite.addTest(TestIKB('test_action_on_individual'))
  suite.addTest(TestIKB('test_get_blood_sample'))
  suite.addTest(TestIKB('test_get_dna_sample'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

