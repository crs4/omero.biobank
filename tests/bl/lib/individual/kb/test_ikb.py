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
  def setUp(self):
    self.ikb = iKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)
    self.skb = sKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)

  def tearDown(self):
    pass

  def test_individual(self):
    gmap = self.ikb.get_gender_table()
    i = self.ikb.Individual(gmap['MALE'])
    f = self.ikb.Individual(gmap['MALE'])
    m = self.ikb.Individual(gmap['FEMALE'])
    jf = self.ikb.save(f)
    jm = self.ikb.save(m)
    try:
      i.father, i.mother = jf, jm
      j = self.ikb.save(i)
      self.assertEqual(j.id, i.id)
      self.assertEqual(j.father.id, f.id)
      self.assertEqual(j.mother.id, m.id)
    finally:
      self.ikb.delete(j)
      self.ikb.delete(jf)
      self.ikb.delete(jm)

  def test_enrollment(self):
    study_label = 'foobar-%f' % time.time()
    enrol_label = 'boobar-%f' % time.time()
    gmap = self.ikb.get_gender_table()
    i = self.ikb.Individual(gmap['MALE'])
    s = self.skb.Study(study_label)
    e = self.ikb.Enrollment()
    #--
    i = self.ikb.save(i)
    s = self.skb.save(s)
    try:
      e.study = s
      e.individual = i
      e.studyCode = enrol_label
      e = self.ikb.save(e)
      self.assertEqual(e.individual.id, i.id)
      self.assertEqual(e.study.id, s.id)
      self.assertEqual(e.studyCode, enrol_label)
    finally:
      self.ikb.delete(e)
      self.ikb.delete(i)
      self.ikb.delete(s)

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestIKB('test_individual'))
  suite.addTest(TestIKB('test_enrollment'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

