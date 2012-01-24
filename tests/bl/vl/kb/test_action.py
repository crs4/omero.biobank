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

  def test_study(self):
    conf, s = self.create_study()
    self.kill_list.append(s.save())
    self.check_object(s, conf, self.kb.Study)

  def test_study_ops(self):
    conf, s = self.create_study()
    s.save()
    xs = self.kb.get_study(conf['label'])
    self.assertTrue(not xs is None)
    self.assertEqual(xs.id, s.id)
    self.assertEqual(xs.label, s.label)
    self.kb.delete(s)
    self.assertEqual(self.kb.get_study(conf['label']), None)

  def test_device(self):
    conf, d = self.create_device()
    self.kill_list.append(d.save())
    self.check_object(d, conf, self.kb.Device)

  def test_hardware_device(self):
    conf, d = self.create_hardware_device()
    self.kill_list.append(d.save())
    self.check_object(d, conf, self.kb.HardwareDevice)

  def test_device_ops(self):
    conf, d = self.create_device()
    d.save()
    xs = self.kb.get_device(conf['label'])
    self.assertTrue(not xs is None)
    self.check_object(xs, conf, self.kb.Device)
    self.kb.delete(d)
    self.assertEqual(self.kb.get_device(conf['label']), None)

  def test_action_setup(self):
    conf, a = self.create_action_setup()
    self.kill_list.append(a.save())
    self.check_object(a, conf, self.kb.ActionSetup)

  def test_action(self):
    conf, action = self.create_action()
    self.kill_list.append(action.save())
    self.check_object(action, conf, self.kb.Action)

  def test_action_on_vessel(self):
    conf, action = self.create_action_on_vessel()
    self.kill_list.append(action.save())
    self.check_object(action, conf, self.kb.ActionOnVessel)

  def test_action_on_data_sample(self):
    conf, action = self.create_action_on_data_sample()
    self.kill_list.append(action.save())
    self.check_object(action, conf, self.kb.ActionOnDataSample)

  def test_action_on_data_collection_item(self):
    conf, action = self.create_action_on_data_collection_item()
    self.kill_list.append(action.save())
    self.check_object(action, conf, self.kb.ActionOnDataSample)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestKB('test_study'))
  suite.addTest(TestKB('test_study_ops'))
  suite.addTest(TestKB('test_device'))
  suite.addTest(TestKB('test_hardware_device'))
  suite.addTest(TestKB('test_device_ops'))
  suite.addTest(TestKB('test_action_setup'))
  suite.addTest(TestKB('test_action'))
  suite.addTest(TestKB('test_action_on_vessel'))
  suite.addTest(TestKB('test_action_on_data_sample'))
  suite.addTest(TestKB('test_action_on_data_collection_item'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
