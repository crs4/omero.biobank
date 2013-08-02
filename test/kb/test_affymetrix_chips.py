# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, unittest, logging
logging.basicConfig(level=logging.ERROR)

from bl.vl.kb import KnowledgeBase as KB
from affymetrix_chips_creator import KBACObjectCreator
from enum_base import EnumBase


OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")


class TestKB(KBACObjectCreator):

  def __init__(self, name):
    super(TestKB, self).__init__(name)
    self.kill_list = []

  def setUp(self):
    self.kb = KB(driver='omero')(
      OME_HOST, OME_USER, OME_PASS, extra_modules="affymetrix_chips")

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

  def test_affymetrix_array(self):
    conf, a = self.create_affymetrix_array()
    self.kill_list.append(a.save())
    self.check_object(a, conf, self.kb.AffymetrixArray)

  def test_affymetrix_cel(self):
    conf, a = self.create_affymetrix_cel()
    self.kill_list.append(a.save())
    self.check_object(a, conf, self.kb.AffymetrixCel)

class TestEnums(EnumBase):

  def __init__(self, name):
    super(TestEnums, self).__init__(name)
    self.kill_list = []
    self.enum_names = [
      'AffymetrixAssayType',
      'AffymetrixCelArrayType'
      ]

  def setUp(self):
    self.kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASS)

  def test_enums(self):
    self._check_enums()


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestKB('test_affymetrix_array'))
  suite.addTest(TestKB('test_affymetrix_cel'))
  suite.addTest(TestEnums('test_enums'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
