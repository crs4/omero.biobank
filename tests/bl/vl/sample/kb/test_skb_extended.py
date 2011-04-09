import os, unittest, time
import itertools as it
from bl.vl.sample.kb import KBError
from bl.vl.sample.kb import KnowledgeBase as sKB

import logging
logging.basicConfig(level=logging.ERROR)

from skb_object_creator import SKBObjectCreator

OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")

class TestSKBExtended(SKBObjectCreator, unittest.TestCase):
  " "
  def __init__(self, name):
    super(TestSKBExtended, self).__init__(name)
    self.kill_list = []

  def setUp(self):
    self.skb = sKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)
    self.acat_map   = self.skb.get_action_category_table()
    self.outcome_map = self.skb.get_result_outcome_table()
    self.sstatus_map = self.skb.get_sample_status_table()
    self.dtype_map   = self.skb.get_data_type_table()

  def tearDown(self):
    self.kill_list.reverse()
    for x in self.kill_list:
      self.skb.delete(x)
    self.kill_list = []

  def check_object(self, o, conf, otype):
    try:
      self.assertTrue(isinstance(o, otype))
      for k in conf.keys():
        v = conf[k]
        if hasattr(v, 'ome_obj'):
          self.assertEqual(getattr(o, k).id, v.id)
        elif hasattr(v, '_id'):
          self.assertEqual(getattr(o, k)._id, v._id)
        else:
          self.assertEqual(getattr(o, k), v)
    except:
      pass

  def test_sample_chain(self):
    conf, sample = self.create_sample_chain()
    sample = self.skb.save(sample)
    self.kill_list.append(sample)
    root = self.skb.get_root(sample)
    self.assertEqual(root.__class__, self.skb.BloodSample)
    #-
    derived = self.skb.get_descendants(root)
    self.assertTrue(len(derived), 2)
    derived = self.skb.get_descendants(root, self.skb.DataSample)
    self.assertTrue(len(derived), 1)
    self.assertTrue(derived[0].__class__, self.skb.DataSample)
    derived = self.skb.get_descendants(root, self.skb.DNASample)
    self.assertTrue(len(derived), 1)
    self.assertTrue(derived[0].__class__, self.skb.DNASample)


  def test_get_device(self):
    conf, device = self.create_device()
    device = self.skb.save(device)
    d2 = self.skb.get_device(conf['vendor'], conf['model'], conf['release'])
    self.assertEqual(d2.id, device.id)
    self.skb.delete(device)

  def test_get_devices(self):
    dev_by_vid = {}
    for i in range(3):
      conf, device = self.create_device()
      device = self.skb.save(device)
      dev_by_vid[device.id] = device
      self.kill_list.append(device)
    devs = self.skb.get_devices()
    found = 0
    for d in devs:
      if dev_by_vid.has_key(d.id):
        found += 1
        self.assertEqual(dev_by_vid[d.id].vendor, d.vendor)
        self.assertEqual(dev_by_vid[d.id].model, d.model)
        self.assertEqual(dev_by_vid[d.id].release, d.release)
    self.assertEqual(len(dev_by_vid), found)

  def test_get_titer_plates(self):
    plates_by_vid = {}
    for i in range(3):
      conf, tplate = self.create_titer_plate()
      tplate = self.skb.save(tplate)
      self.kill_list.append(tplate)
      plates_by_vid[tplate.id] = tplate
    tps = self.skb.get_titer_plates()
    found = 0
    for tp in tps:
      if plates_by_vid.has_key(tp.id):
        found += 1
    self.assertEqual(found, len(plates_by_vid))

  def test_get_wells_of_plate(self):
    pw_map = {}
    conf, tplate = self.create_titer_plate()
    tplate = self.skb.save(tplate)
    self.kill_list.append(tplate)
    for r in range(tplate.rows):
      for c in range(tplate.columns):
        conf, pw = self.create_plate_well(container=tplate, row=r, column=c)
        pw = self.skb.save(pw)
        self.kill_list.append(pw)
        pw_map[pw.id] = pw
    wells = self.skb.get_wells_of_plate(tplate)
    for w in wells:
      self.assertTrue(pw_map.has_key(w.id))

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestSKBExtended('test_sample_chain'))
  suite.addTest(TestSKBExtended('test_get_device'))
  suite.addTest(TestSKBExtended('test_get_devices'))
  suite.addTest(TestSKBExtended('test_get_titer_plates'))
  suite.addTest(TestSKBExtended('test_get_wells_of_plate'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

