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

  def test_container(self):
    conf, c = self.create_container()
    self.kill_list.append(c.save())
    self.check_object(c, conf, self.kb.Container)

  def test_slotted_container(self):
    conf, c = self.create_slotted_container()
    self.kill_list.append(c.save())
    self.check_object(c, conf, self.kb.SlottedContainer)

  def test_titer_plate(self):
    conf, c = self.create_titer_plate()
    self.kill_list.append(c.save())
    self.check_object(c, conf, self.kb.TiterPlate)

  def test_data_collection(self):
    conf, c = self.create_data_collection()
    self.kill_list.append(c.save())
    self.check_object(c, conf, self.kb.DataCollection)

  def test_data_collection_item(self):
    conf, c = self.create_data_collection_item()
    self.kill_list.append(c.save())
    self.check_object(c, conf, self.kb.DataCollectionItem)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestKB('test_container'))
  suite.addTest(TestKB('test_slotted_container'))
  suite.addTest(TestKB('test_titer_plate'))
  suite.addTest(TestKB('test_data_collection'))
  suite.addTest(TestKB('test_data_collection_item'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
