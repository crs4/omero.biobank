# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, unittest, logging
logging.basicConfig(level=logging.ERROR)

from bl.vl.kb import KnowledgeBase as KB
from kb_object_creator import KBObjectCreator


OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")


class TestKB(KBObjectCreator):

  def __init__(self, name):
    super(TestKB, self).__init__(name)
    self.kill_list = []

  def setUp(self):
    self.kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASS)

  def tearDown(self):
    self.kill_list.reverse()
    for x in self.kill_list:
      self.kb.delete(x)
    self.kill_list = []

  def check_object(self, o, conf, otype):
    try:
      self.assertTrue(isinstance(o, otype))
      for k in conf.keys():
        v = conf[k]
        # FIXME this is omero specific...
        if hasattr(v, 'ome_obj'):
          self.assertEqual(getattr(o, k).id, v.id)
          self.assertEqual(type(getattr(o, k)), type(v))
        elif hasattr(v, '_id'):
          self.assertEqual(getattr(o, k)._id, v._id)
        else:
          self.assertEqual(getattr(o, k), v)
    except:
      pass

  def test_individual(self):
    conf, i = self.create_individual()
    self.kill_list.append(i.save())
    self.check_object(i, conf, self.kb.Individual)

  def test_enrollment(self):
    conf, e = self.create_enrollment()
    self.kill_list.append(e.save())
    self.check_object(e, conf, self.kb.Enrollment)

  def test_enrollment_ops(self):
    conf, e = self.create_enrollment()
    e.save()
    study = e.study
    xe = self.kb.get_enrollment(study, conf['studyCode'])
    self.assertTrue(not xe is None)
    self.assertEqual(xe.id, e.id)
    self.kb.delete(e)
    self.assertEqual(self.kb.get_enrollment(study, conf['studyCode']), None)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestKB('test_individual'))
  suite.addTest(TestKB('test_enrollment'))
  suite.addTest(TestKB('test_enrollment_ops'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
