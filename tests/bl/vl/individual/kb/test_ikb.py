import os, unittest, time
import itertools as it
from bl.vl.individual.kb import KnowledgeBase as iKB
from bl.vl.sample.kb     import KnowledgeBase as sKB

from skb_object_creator import SKBObjectCreator

import logging
logging.basicConfig(level=logging.ERROR)


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
    self.acat_map   = self.skb.get_action_category_table()
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

  def create_individual(self, gender='MALE', action=None):
    gmap = self.ikb.get_gender_table()
    #-
    if action is None:
      conf, action = self.create_action()
      action = self.skb.save(action)
      self.kill_list.append(action)
    conf = {'gender' : gmap[gender], 'action' : action}
    i = self.ikb.Individual(gender=conf['gender'])
    self.configure_object(i, conf)
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

  def create_action_on_individual(self, individual=None):
    return self.create_action(action=self.ikb.ActionOnIndividual(),
                              target=individual)

  def test_orphan(self):
    conf, i = self.create_individual('MALE')
    i = self.ikb.save(i)
    self.kill_list.append(i)
    self.check_object(i, conf, self.ikb.Individual)

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
    self.kill_list.append(i)
    self.check_object(i, conf, self.ikb.Individual)

  def test_enrollment(self):
    conf, e = self.create_enrollment()
    e = self.ikb.save(e)
    self.kill_list.append(e)
    self.check_object(e, conf, self.ikb.Enrollment)


  def test_action_on_individual(self):
    conf, i = self.create_individual('MALE')
    i = self.ikb.save(i)
    self.kill_list.append(i)
    conf, action = self.create_action_on_individual(individual=i)
    action = self.ikb.save(action)
    self.kill_list.append(action)
    self.check_object(action, conf, self.ikb.ActionOnIndividual)

  def create_individual_sample_chain(self):
    conf, i = self.create_individual('MALE')
    i = self.ikb.save(i)
    self.kill_list.append(i)
    conf, action = self.create_action_on_individual(individual=i)
    action = self.ikb.save(action)
    self.kill_list.append(action)
    #--
    conf, data_sample = self.create_sample_chain(root_action=action)
    return conf, data_sample

  def test_get_individual_chain(self):
    conf, data_sample = self.create_individual_sample_chain()
    data_sample = self.skb.save(data_sample)
    self.kill_list.append(data_sample)
    #-
    root = self.skb.get_root(data_sample)
    self.assertTrue(isinstance(root, self.ikb.Individual))
    self.assertEqual(type(root), self.ikb.Individual)
    #-
    blood_samples = self.skb.get_descendants(root, self.skb.BloodSample)
    for bs in blood_samples:
      self.assertEqual(type(bs), self.skb.BloodSample)
    dna_samples =  self.skb.get_descendants(root, self.skb.DNASample)
    for ds in dna_samples:
      self.assertEqual(type(ds), self.skb.DNASample)

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
  suite.addTest(TestIKB('test_get_individual_chain'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

