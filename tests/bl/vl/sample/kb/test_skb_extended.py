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

  # def test_sample_chain(self):
  #   conf, sample = self.create_sample_chain()
  #   sample = self.skb.save(sample)
  #   self.kill_list.append(sample)
  #   root = self.skb.get_root(sample)
  #   self.assertEqual(root.__class__, self.skb.BloodSample)
  #   #-
  #   derived = self.skb.get_descendants(root)
  #   self.assertTrue(len(derived), 2)
  #   derived = self.skb.get_descendants(root, self.skb.DataSample)
  #   self.assertTrue(len(derived), 1)
  #   self.assertTrue(derived[0].__class__, self.skb.DataSample)
  #   derived = self.skb.get_descendants(root, self.skb.DNASample)
  #   self.assertTrue(len(derived), 1)
  #   self.assertTrue(derived[0].__class__, self.skb.DNASample)


  def test_get_device(self):
    conf, device = self.create_device()
    device = self.skb.save(device)
    d2 = self.skb.get_device(conf['label'])
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
        self.assertEqual(dev_by_vid[d.id].label, d.label)
        self.assertEqual(dev_by_vid[d.id].maker, d.maker)
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
    self.assertEqual(len(wells), tplate.rows * tplate.columns)
    for w in wells:
      self.assertTrue(pw_map.has_key(w.id))
      self.assertEqual(type(w), self.skb.PlateWell)

  def test_get_dna_sample(self):
    saved = {}
    for i in range(10):
      conf, dna_sample = self.create_dna_sample()
      dna_sample = self.skb.save(dna_sample)
      self.kill_list.append(dna_sample)
      saved[dna_sample.barcode] = dna_sample
    for k in saved.keys():
      sample = self.skb.get_dna_sample(barcode=k)
      self.assertEqual(sample.omero_id, saved[k].omero_id)
      self.assertEqual(sample.id, saved[k].id)

  def test_get_data_objects(self):
    saved, data_sample= {}, None
    for i in range(10):
      conf, do = self.create_data_object(data_sample=data_sample)
      do = self.skb.save(do)
      self.kill_list.append(do)
      saved[do.path] = do
      data_sample = do.sample
    #-
    res = self.skb.get_data_objects(data_sample)
    counts = {}
    self.assertTrue(len(saved), len(res))
    for r in res:
      self.assertTrue(saved.has_key(r.path))
      self.assertFalse(counts.has_key(r.path))
      counts[r.path] = 1

  def test_find_all_by_query(self):
    conf, data_sample = self.create_sample_chain()
    data_sample = self.skb.save(data_sample)
    self.kill_list.append(data_sample)
    dss = self.skb.get_bio_samples(self.skb.DataSample)
    query = """select a from ActionOnSample a
               join fetch a.target t
               where a.id in (select da.id
                              from DataSample d
                              join d.action as da
                              )
               """
    res = self.skb.find_all_by_query(query, {}, self.skb.Action)
    actions = {}
    for a in res:
      actions[a.omero_id] = a
    for d in dss:
      self.assertTrue(actions.has_key(d.action.omero_id))

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestSKBExtended('test_get_device'))
  suite.addTest(TestSKBExtended('test_get_devices'))
  suite.addTest(TestSKBExtended('test_get_titer_plates'))
  suite.addTest(TestSKBExtended('test_get_wells_of_plate'))
  suite.addTest(TestSKBExtended('test_get_dna_sample'))
  suite.addTest(TestSKBExtended('test_get_data_objects'))
  suite.addTest(TestSKBExtended('test_find_all_by_query'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

