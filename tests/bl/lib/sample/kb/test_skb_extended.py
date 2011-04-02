import os, unittest, time
import itertools as it
from bl.lib.sample.kb import KBError
from bl.lib.sample.kb import KnowledgeBase as sKB

import logging
logging.basicConfig(level=logging.DEBUG)

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
    self.atype_map   = self.skb.get_action_type_table()
    self.outcome_map = self.skb.get_result_outcome_table()
    self.sstatus_map = self.skb.get_sample_status_table()
    self.dtype_map   = self.skb.get_data_type_table()

  def tearDown(self):
    self.kill_list.reverse()
    for x in self.kill_list:
      #print 'deleting %s[%s]' % (type(x), x.id)
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
    devs = self.skb.get_devices()
    for d in devs:
      self.assertTrue(dev_by_vid.has_key(d.id))
      self.assertEqual(dev_by_vid[d.id].vendor, d.vendor)
      self.assertEqual(dev_by_vid[d.id].model, d.model)
      self.assertEqual(dev_by_vid[d.id].release, d.release)
      self.skb.delete(d)

  def test_get_titer_plates(self):
    plates_by_vid = {}
    for i in range(3):
      conf, tplate = self.create_titer_plate()
      tplate = self.skb.save(tplate)
      self.kill_list.append(tplate)
      plates_by_vid[tplate.id] = tplate
    tps = self.skb.get_titer_plates()
    for tp in tps:
      self.assertTrue(plates_by_vid.has_key(tp.id))

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
  suite.addTest(TestSKBExtended('test_get_device'))
  suite.addTest(TestSKBExtended('test_get_devices'))
  suite.addTest(TestSKBExtended('test_get_titer_plates'))
  suite.addTest(TestSKBExtended('test_get_wells_of_plate'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
