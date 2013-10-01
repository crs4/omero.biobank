# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, unittest, logging
logging.basicConfig(level=logging.ERROR)

from bl.vl.kb import KnowledgeBase as KB
from illumina_chips_creator import KBICObjectCreator
from enum_base import EnumBase


OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")


class TestKB(KBICObjectCreator):

  def __init__(self, name):
    super(TestKB, self).__init__(name)
    self.kill_list = []

  def setUp(self):
    self.kb = KB(driver='omero')(
      OME_HOST, OME_USER, OME_PASS, extra_modules="illumina_chips")

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

  def test_illumina_array_of_arrays(self):
    conf, a = self.create_illumina_array_of_arrays(rows=6, cols=2)
    self.kill_list.append(a.save())
    self.check_object(a, conf, self.kb.IlluminaArrayOfArrays)

  def test_illumina_bead_chip_measures(self):
    channels = {}
    for c in ['red_channel', 'green_channel']:
      conf, m = self.create_illumina_bead_chip_measure()
      self.kill_list.append(m.save())
      self.check_object(m, conf, self.kb.IlluminaBeadChipMeasure)
      channels[c] = m
    conf, ms = self.create_illumina_bead_chip_measures(**channels)
    self.kill_list.append(ms.save())
    self.check_object(ms, conf, self.kb.IlluminaBeadChipMeasures)

  def test_illumina_bead_chip_array(self):
    conf, a = self.create_illumina_array_of_arrays(rows=6, cols=2)
    self.kill_list.append(a.save())
    conf, c = self.create_illumina_bead_chip_array(label="R03C02",
                                                   array_of_arrays=a)
    self.kill_list.append(c.save())
    self.check_object(c, conf, self.kb.IlluminaBeadChipArray)

  def test_illumina_bead_chip_array_errors(self):
    conf, a = self.create_illumina_array_of_arrays(rows=6, cols=2)
    self.kill_list.append(a.save())
    conf, c = self.create_illumina_bead_chip_array(label="R03C02",
                                                   array_of_arrays=a)
    self.kill_list.append(c.save())
    with self.assertRaises(ValueError):
      self.create_illumina_bead_chip_array("R03C02x", a)
    with self.assertRaises(ValueError):
        self.create_illumina_bead_chip_array("R03C03", a)
    with self.assertRaises(ValueError):
        self.create_illumina_bead_chip_array("R22C03", a)
    with self.assertRaises(ValueError):
        self.create_illumina_bead_chip_array("R01C01", a, 12)


class TestEnums(EnumBase):

  def __init__(self, name):
    super(TestEnums, self).__init__(name)
    self.kill_list = []
    self.enum_names = [
      'IlluminaBeadChipAssayType',
      ]

  def setUp(self):
    self.kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASS)

  def test_enums(self):
    self._check_enums()


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestKB('test_illumina_array_of_arrays'))
  suite.addTest(TestKB('test_illumina_bead_chip_array'))
  suite.addTest(TestKB('test_illumina_bead_chip_array_errors'))
  suite.addTest(TestKB('test_illumina_bead_chip_measures'))
  suite.addTest(TestEnums('test_enums'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
