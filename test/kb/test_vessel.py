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

  def test_vessel(self):
    conf, v = self.create_vessel()
    self.kill_list.append(v.save())
    self.check_object(v, conf, self.kb.Vessel)

  def test_tube(self):
    conf, v = self.create_tube()
    self.kill_list.append(v.save())
    self.check_object(v, conf, self.kb.Tube)

  def test_plate_well(self):
    conf, p = self.create_titer_plate()
    self.kill_list.append(p.save())
    conf, v = self.create_plate_well(p)
    self.kill_list.append(v.save())
    self.check_object(v, conf, self.kb.PlateWell)

    conf, v = self.create_plate_well(p, label='B01')
    self.kill_list.append(v.save())
    self.assertEqual(v.slot, p.columns+1)

    conf, v = self.create_plate_well(p, label='A02')
    self.kill_list.append(v.save())
    self.assertEqual(v.slot, 2)

    conf, v = self.create_plate_well(p, label='D3')
    self.kill_list.append(v.save())
    self.assertEqual(v.slot, 3*p.columns + 3)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestKB('test_vessel'))
  suite.addTest(TestKB('test_tube'))
  suite.addTest(TestKB('test_plate_well'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
