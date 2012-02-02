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

  def test_enums(self):
    for ename in ['ContainerStatus', 'AffymetrixCelArrayType',
                  'IlluminaBeadChipAssayType', 'DataSampleStatus',
                  'ActionCategory', 'Gender', 'VesselContent']:
      enum_klass = getattr(self.kb, ename)
      enum_klass.map_enums_values(self.kb)
      for x in enum_klass.__enums__:
        self.assertEqual(x.enum_label(), x.ome_obj.value.val)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestKB('test_enums'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
