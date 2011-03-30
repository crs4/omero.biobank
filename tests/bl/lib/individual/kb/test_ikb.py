import os, unittest, time
import itertools as it
from bl.lib.individual.kb import KnowledgeBase as iKB
from bl.lib.sample.kb     import KnowledgeBase as sKB

import logging
logging.basicConfig(level=logging.DEBUG)


OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")

class TestIKB(unittest.TestCase):
  def __init__(self, name):
    self.kill_list = []
    super(TestIKB, self).__init__(name)

  def setUp(self):
    self.ikb = iKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)
    self.skb = sKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)

  def tearDown(self):
    self.kill_list.reverse()
    for x in self.kill_list:
      #print 'deleting %s[%s]' % (type(x), x.id)
      self.skb.delete(x)
    self.kill_list = []

  def configure_object(self, o, conf):
    for k in conf.keys():
      setattr(o, k, conf[k])
    conf['id'] = o.id

  def check_object(self, o, conf, otype):
    try:
      self.assertTrue(isinstance(o, otype))
      for k in conf.keys():
        v = conf[k]
        if hasattr(v, 'ome_obj'):
          self.assertEqual(getattr(o, k).id, v.id)
        elif hasattr(v, '_id'):
          self.assertEqual(getattr(o, k)._id, v._id)
        else:
          self.assertEqual(getattr(o, k), v)
    except:
      pass

  def create_individual(self, gender):
    gmap = self.ikb.get_gender_table()
    conf = {'gender' : gmap[gender]}
    i = self.ikb.Individual(gender=conf['gender'])
    return conf, i

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

  def create_study(self):
    conf = {'label' : 'foobar_%f' % time.time()}
    s = self.skb.Study(label=conf['label'])
    conf['id'] = s.id
    return conf, s

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

  def test_enrollment(self):
    conf, e = self.create_enrollment()
    e = self.ikb.save(e)
    self.check_object(e, conf, self.ikb.Enrollment)
    self.ikb.delete(e)

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestIKB('test_orphan'))
  suite.addTest(TestIKB('test_with_parents'))
  suite.addTest(TestIKB('test_enrollment'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

