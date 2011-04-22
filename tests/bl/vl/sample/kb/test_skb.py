import os, unittest, time
import itertools as it
from bl.vl.sample.kb import KBError
from bl.vl.sample.kb import KnowledgeBase as sKB

import logging
logging.basicConfig(level=logging.DEBUG)

from skb_object_creator import SKBObjectCreator

OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")

class TestSKB(SKBObjectCreator, unittest.TestCase):
  " "
  def __init__(self, name):
    super(TestSKB, self).__init__(name)
    self.kill_list = []

  def setUp(self):
    self.skb = sKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)
    self.acat_map    = self.skb.get_action_category_table()
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
          self.assertEqual(type(getattr(o, k)), type(v))
        elif hasattr(v, '_id'):
          self.assertEqual(getattr(o, k)._id, v._id)
        else:
          self.assertEqual(getattr(o, k), v)
    except:
      pass

  def test_study(self):
    conf, s = self.create_study()
    s = self.skb.save(s)
    self.check_object(s, conf, self.skb.Study)
    #--
    xs = self.skb.get_study_by_label(conf['label'])
    self.assertTrue(not xs is None)
    self.assertEqual(xs.id, s.id)
    self.assertEqual(xs.label, s.label)
    self.skb.delete(s)
    xs = self.skb.get_study_by_label(conf['label'])
    self.assertTrue(xs is None)

  def test_data_object(self):
    conf, do = self.create_data_object()
    do = self.skb.save(do)
    self.kill_list.append(do)
    self.check_object(do, conf, self.skb.DataObject)

  def test_device(self):
    conf, d = self.create_device()
    d = self.skb.save(d)
    self.check_object(d, conf, self.skb.Device)
    xs = self.skb.get_device(conf['label'])
    self.assertTrue(not xs is None)
    self.check_object(xs, conf, self.skb.Device)
    self.skb.delete(d)
    self.assertEqual(self.skb.get_device(conf['label']),
                     None)

  def test_action_setup(self):
    conf, a = self.create_action_setup()
    a = self.skb.save(a)
    self.check_object(a, conf, self.skb.ActionSetup)
    self.skb.delete(a)

  def test_action(self):
    conf, action = self.create_action()
    action = self.skb.save(action)
    self.check_object(action, conf, self.skb.Action)
    self.skb.delete(action)


  def test_result(self):
    conf, result = self.create_result()
    result = self.skb.save(result)
    self.check_object(result, conf, self.skb.Result)
    self.skb.delete(result)


  def test_object_deletion(self):
    conf, action = self.create_action_on_container()
    action = self.skb.save(action)
    self.check_object(action, conf, self.skb.ActionOnSamplesContainer)
    #
    a_id = action.id
    rows = self.skb.get_table_rows(self.skb.ACTION_TABLE, selector='(a_vid == "%s")' % a_id)
    self.assertEqual(len(rows), 1)
    self.skb.delete(action)
    rows = self.skb.get_table_rows(self.skb.ACTION_TABLE, selector='(a_vid == "%s")' % a_id)
    self.assertFalse(rows)
    #-
    conf, result = self.create_result()
    result = self.skb.save(result)
    self.check_object(result, conf, self.skb.Result)
    t_id = result.id
    rows = self.skb.get_table_rows(self.skb.TARGET_TABLE, selector='(t_vid == "%s")' % t_id)
    self.assertEqual(len(rows), 1)
    self.skb.delete(result)
    rows = self.skb.get_table_rows(self.skb.TARGET_TABLE, selector='(t_vid == "%s")' % t_id)
    self.assertFalse(rows)


  def test_sample(self):
    conf, sample = self.create_sample()
    sample = self.skb.save(sample)
    self.check_object(sample, conf, self.skb.Sample)
    self.skb.delete(sample)

  def test_data_sample(self):
    conf, data_sample = self.create_data_sample()
    data_sample = self.skb.save(data_sample)
    self.check_object(data_sample, conf, self.skb.DataSample)
    self.skb.delete(data_sample)

  def test_affymetrix_cel(self):
    conf, data_sample = self.create_affymetrix_cel()
    data_sample = self.skb.save(data_sample)
    self.check_object(data_sample, conf, self.skb.AffymetrixCel)
    self.skb.delete(data_sample)

  def test_snp_markers_set(self):
    conf, mset = self.create_snp_markers_set()
    mset = self.skb.save(mset)
    self.check_object(mset, conf, self.skb.SNPMarkersSet)
    mx = self.skb.get_snp_markers_set(conf['maker'], conf['model'], conf['release'])
    self.assertEqual(mx.id, mset.id)
    self.skb.delete(mset)
    mx = self.skb.get_snp_markers_set(conf['maker'], conf['model'], conf['release'])
    self.assertTrue(mx is None)

  def test_genotype_data_sample(self):
    conf, obj = self.create_genotype_data_sample()
    obj = self.skb.save(obj)
    self.check_object(obj, conf, self.skb.GenotypeDataSample)
    self.skb.delete(obj)

  def test_bio_sample(self):
    conf, bio_sample = self.create_bio_sample()
    bio_sample = self.skb.save(bio_sample)
    self.check_object(bio_sample, conf, self.skb.BioSample)
    self.skb.delete(bio_sample)

  def test_blood_sample(self):
    conf, blood_sample = self.create_blood_sample()
    blood_sample = self.skb.save(blood_sample)
    self.check_object(blood_sample, conf, self.skb.BloodSample)
    self.skb.delete(blood_sample)

  def test_dna_sample(self):
    conf, dna_sample = self.create_dna_sample()
    dna_sample = self.skb.save(dna_sample)
    self.check_object(dna_sample, conf, self.skb.DNASample)
    self.skb.delete(dna_sample)

  def test_serum_sample(self):
    conf, serum_sample = self.create_serum_sample()
    serum_sample = self.skb.save(serum_sample)
    self.check_object(serum_sample, conf, self.skb.SerumSample)
    self.skb.delete(serum_sample)

  def test_samples_container(self):
    conf, sc = self.create_samples_container()
    sc = self.skb.save(sc)
    self.check_object(sc, conf, self.skb.SamplesContainer)
    self.skb.delete(sc)

  def test_titer_plate(self):
    conf, tp = self.create_titer_plate()
    tp = self.skb.save(tp)
    self.check_object(tp, conf, self.skb.TiterPlate)
    self.skb.delete(tp)

  def test_action_on_sample(self):
    conf, sample = self.create_sample()
    sample = self.skb.save(sample)
    self.kill_list.append(sample)
    #--
    conf, action = self.create_action_on_sample(sample=sample)
    action = self.skb.save(action)
    self.kill_list.append(action)
    #--
    self.check_object(action, conf, self.skb.ActionOnSample)

  def test_action_on_container(self):
    conf, action = self.create_action_on_container()
    action = self.skb.save(action)
    self.check_object(action, conf, self.skb.ActionOnSamplesContainer)
    self.skb.delete(action)

  def test_container_slot(self):
    conf, container_slot = self.create_container_slot()
    container_slot = self.skb.save(container_slot)
    self.check_object(container_slot, conf, self.skb.SamplesContainerSlot)
    self.skb.delete(container_slot)
    return conf, container_slot

  def test_plate_well_dna(self):
    conf, dna_sample = self.create_dna_sample()
    dna_sample = self.skb.save(dna_sample)
    self.kill_list.append(dna_sample)
    #-
    conf, plate_well = self.create_plate_well(sample=dna_sample)
    plate_well = self.skb.save(plate_well)
    self.check_object(plate_well, conf, self.skb.PlateWell)
    self.kill_list.append(plate_well)

  def test_plate_well(self):
    conf, plate_well = self.create_plate_well()
    plate_well = self.skb.save(plate_well)
    self.check_object(plate_well, conf, self.skb.PlateWell)
    self.skb.delete(plate_well)

  def test_action_on_container_slot(self):
    conf, action = self.create_action_on_container_slot()
    action = self.skb.save(action)
    self.check_object(action, conf, self.skb.ActionOnSamplesContainerSlot)
    self.skb.delete(action)

  def test_data_collection(self):
    conf, data_collection = self.create_data_collection()
    data_collection = self.skb.save(data_collection)
    self.kill_list.append(data_collection)
    self.check_object(data_collection, conf, self.skb.DataCollection)

  def test_data_collection_item(self):
    conf, data_collection_item = self.create_data_collection_item()
    data_collection_item = self.skb.save(data_collection_item)
    self.kill_list.append(data_collection_item)
    self.check_object(data_collection_item, conf, self.skb.DataCollectionItem)

  def test_action_on_data_collection(self):
    conf, action = self.create_action_on_data_collection()
    action = self.skb.save(action)
    self.check_object(action, conf, self.skb.ActionOnDataCollection)
    self.skb.delete(action)

  def test_action_on_data_collection_item(self):
    conf, action = self.create_action_on_data_collection()
    action = self.skb.save(action)
    self.check_object(action, conf, self.skb.ActionOnDataCollection)
    self.skb.delete(action)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestSKB('test_study'))
  suite.addTest(TestSKB('test_data_object'))
  suite.addTest(TestSKB('test_device'))
  suite.addTest(TestSKB('test_action_setup'))
  suite.addTest(TestSKB('test_action'))
  suite.addTest(TestSKB('test_result'))
  suite.addTest(TestSKB('test_sample'))
  suite.addTest(TestSKB('test_data_sample'))
  suite.addTest(TestSKB('test_snp_markers_set'))
  suite.addTest(TestSKB('test_genotype_data_sample'))
  suite.addTest(TestSKB('test_affymetrix_cel'))
  suite.addTest(TestSKB('test_bio_sample'))
  suite.addTest(TestSKB('test_blood_sample'))
  suite.addTest(TestSKB('test_dna_sample'))
  suite.addTest(TestSKB('test_serum_sample'))
  suite.addTest(TestSKB('test_samples_container'))
  suite.addTest(TestSKB('test_titer_plate'))
  suite.addTest(TestSKB('test_container_slot'))
  suite.addTest(TestSKB('test_plate_well'))
  suite.addTest(TestSKB('test_plate_well_dna'))
  suite.addTest(TestSKB('test_action_on_sample'))
  suite.addTest(TestSKB('test_action_on_container'))
  suite.addTest(TestSKB('test_action_on_container_slot'))
  suite.addTest(TestSKB('test_action_on_data_collection'))
  suite.addTest(TestSKB('test_action_on_data_collection_item'))
  suite.addTest(TestSKB('test_data_collection'))
  suite.addTest(TestSKB('test_data_collection_item'))
  suite.addTest(TestSKB('test_object_deletion'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

