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

  def test_data_sample(self):
    conf, ds = self.create_data_sample()
    self.kill_list.append(ds.save())
    self.check_object(ds, conf, self.kb.DataSample)

  def test_snp_markers_set(self):
    conf, sms = self.create_snp_markers_set()
    self.kill_list.append(sms.save())
    self.check_object(sms, conf, self.kb.SNPMarkersSet)

  def test_genotype_data_sample(self):
    conf, gds = self.create_genotype_data_sample()
    self.kill_list.append(gds.save())
    self.check_object(gds, conf, self.kb.GenotypeDataSample)

  def test_data_object(self):
    conf, do = self.create_data_object()
    self.kill_list.append(do.save())
    self.check_object(do, conf, self.kb.DataObject)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestKB('test_data_sample'))
  suite.addTest(TestKB('test_snp_markers_set'))
  suite.addTest(TestKB('test_genotype_data_sample'))
  suite.addTest(TestKB('test_data_object'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
