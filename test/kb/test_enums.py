# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, unittest, logging
logging.basicConfig(level=logging.ERROR)

from bl.vl.kb import KnowledgeBase as KB
from enum_base import EnumBase
from bl.vl.kb.drivers.omero.wrapper import MetaWrapper

OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")


class TestEnums(EnumBase):

  def __init__(self, name):
    super(TestEnums, self).__init__(name)
    self.kill_list = []
    self.enum_names = []

  def setUp(self):
    self.kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASS)
    def is_enum(k):
      a = getattr(self.kb, k)
      return type(a) == MetaWrapper and a.is_enum()
    self.enum_names = filter(is_enum, dir(self.kb))

  def test_enums(self):
    self._check_enums()


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestEnums('test_enums'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
