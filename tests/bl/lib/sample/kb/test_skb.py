import os, unittest, time
import itertools as it
from bl.lib.sample.kb import KBError
from bl.lib.sample.kb import KnowledgeBase as sKB

import logging
logging.basicConfig(level=logging.DEBUG)


OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")

class TestSKB(unittest.TestCase):
  def setUp(self):
    self.skb = sKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)
    self.atype_map   = self.skb.get_action_type_table()
    self.outcome_map = self.skb.get_result_outcome_table()
    self.sstatus_map = self.skb.get_sample_status_table()

  def tearDown(self):
    pass

  def test_study(self):
    label = 'foobar_%f' % time.time()
    s = self.skb.Study(label)
    vid = s.id
    s = self.skb.save(s)
    try:
      self.assertEqual(s.label, label)
      self.assertEqual(s.id, vid)
      xs = self.skb.get_study_by_label(label)
      self.assertTrue(not xs is None)
      self.assertEqual(xs.id, s.id)
      self.assertEqual(xs.label, s.label)
    finally:
      self.skb.delete(s)
    xs = self.skb.get_study_by_label(label)
    self.assertTrue(xs is None)

  def test_device(self):
    vendor, model, release = 'foomaker', 'foomodel', '0.2'
    d = self.skb.Device()
    d.vendor  = vendor
    d.model   = model
    d.release = release
    vid = d.id
    d = self.skb.save(d)
    try:
      self.assertEqual(d.vendor, vendor)
      self.assertEqual(d.model, model)
      self.assertEqual(d.release, release)
      self.assertEqual(d.id, vid)
    finally:
      self.skb.delete(d)

  def test_action_setup(self):
    note_text = 'hoooo'
    a = self.skb.ActionSetup()
    vid = a.id
    a.notes = note_text
    a = self.skb.save(a)
    try:
      self.assertEqual(a.id, vid)
      self.assertEqual(a.notes, note_text)
    finally:
      self.skb.delete(a)

  def test_action(self):
    vendor, model, release = 'foomaker', 'foomodel', '0.2'
    operator = 'Alfred E. Neuman'
    study_label = 'foobar_%f' % time.time()
    d = self.skb.Device()
    d.vendor  = vendor
    d.model   = model
    d.release = release
    d = self.skb.save(d)
    #-
    asetup = self.skb.ActionSetup()
    asetup = self.skb.save(asetup)
    #-
    s = self.skb.Study()
    s.label = study_label
    s = self.skb.save(s)
    #-
    desc = 'this is a desc'
    a = self.skb.Action()
    a.setup = asetup
    a.device = d
    atype = self.atype_map['ACQUISITION']
    a.actionType = atype
    a.operator = operator
    a.context = s
    a.description = desc
    vid = a.id
    a = self.skb.save(a)
    try:
      pass
      self.assertEqual(a.id, vid)
      self.assertEqual(a.device.vendor, vendor)
      self.assertEqual(a.device.model, model)
      self.assertEqual(a.device.release, release)
      self.assertEqual(a.actionType._id._val,
                       self.atype_map['ACQUISITION']._id._val)
      self.assertEqual(a.operator, operator)
      self.assertEqual(a.description, desc)
      self.assertEqual(a.context.label, study_label)
    finally:
      self.skb.delete(a)
      self.skb.delete(d)
      self.skb.delete(asetup)
      self.skb.delete(s)


  def helper_result_loader(self, r, ocome, ntext):
    #-
    study_label = 'foobar_%f' % time.time()
    s = self.skb.Study(study_label)
    s = self.skb.save(s)
    #-
    a = self.skb.Action()
    a.actionType = self.atype_map['ACQUISITION']
    a.operator = 'Alfred E. Neuman'
    a.context  = s
    a = self.skb.save(a)
    #-
    r.outcome, r.action, r.notes = ocome, a, ntext
    return s, a, r

  def helper_result_checker(self, r, vid, ocome, a, ntext):
    self.assertEqual(r.id, vid)
    self.assertEqual(r.outcome._id._val, ocome._id._val)
    self.assertEqual(r.action.id, a.id)
    self.assertEqual(r.notes, ntext)

  def test_result(self):
    ocome, ntext = self.outcome_map['PASSED'], 'this is a note'
    r = self.skb.Result()
    s, a, r = self.helper_result_loader(r, ocome, ntext)
    vid = r.id
    r = self.skb.save(r)
    try:
      self.helper_result_checker(r, vid, ocome, a, ntext)
    finally:
      self.skb.delete(r)
      self.skb.delete(a)
      self.skb.delete(s)

  def test_sample(self):
    ocome, ntext = self.outcome_map['PASSED'], 'this is a note'
    r = self.skb.Sample()
    s, a, r = self.helper_result_loader(r, ocome, ntext)
    vid = r.id
    r = self.skb.save(r)
    try:
      self.assertTrue(isinstance(r, self.skb.Result))
      self.helper_result_checker(r, vid, ocome, a, ntext)
    finally:
      self.skb.delete(r)
      self.skb.delete(a)
      self.skb.delete(s)

  def test_data_sample(self):
    ocome, ntext = self.outcome_map['PASSED'], 'this is a note'
    r = self.skb.DataSample()
    s, a, r = self.helper_result_loader(r, ocome, ntext)
    vid = r.id
    r = self.skb.save(r)
    try:
      self.assertTrue(isinstance(r, self.skb.Sample))
      self.helper_result_checker(r, vid, ocome, a, ntext)
    finally:
      self.skb.delete(r)
      self.skb.delete(a)
      self.skb.delete(s)

  def helper_bio_sample_loader(self, r, ocome, sstatus, ntext,
                               lab_lab, bcode, ivol, cvol):
    s, a, r = self.helper_result_loader(r, ocome, ntext)
    r.labLabel, r.barcode  = lab_lab, bcode
    r.initialVolume, r.currentVolume, r.status = ivol, cvol, sstatus
    return s, a, r

  def helper_bio_sample_checker(self, r, vid, ocome, a, ntext,
                               lab_lab, bcode, ivol, cvol):
    self.helper_result_checker(r, vid, ocome, a, ntext)
    self.assertEqual(r.labLabel, lab_lab)
    self.assertEqual(r.barcode, bcode)
    self.assertAlmostEqual(r.initialVolume, ivol, places=5)
    self.assertAlmostEqual(r.currentVolume, cvol, places=5)


  def test_bio_sample(self):
    #-
    ocome, ntext = self.outcome_map['PASSED'], 'this is a note'
    sstatus = self.sstatus_map['USABLE']
    lab_lab, bcode, ivol, cvol = ('lab-label', 'barcode', 12.3, 12.44)
    r = self.skb.BioSample()
    s, a, r = self.helper_bio_sample_loader(r, ocome, sstatus, ntext,
                                            lab_lab, bcode, ivol, cvol)
    #-
    vid = r.id
    r = self.skb.save(r)
    try:
      self.assertTrue(isinstance(r, self.skb.Sample))
      self.helper_bio_sample_checker(r, vid, ocome, a, ntext,
                                     lab_lab, bcode, ivol, cvol)
    finally:
      self.skb.delete(r)
      self.skb.delete(a)
      self.skb.delete(s)

  def test_blood_sample(self):
    #-
    ocome, ntext = self.outcome_map['PASSED'], 'this is a note'
    sstatus = self.sstatus_map['USABLE']
    lab_lab, bcode, ivol, cvol = ('lab-label', 'barcode', 12.3, 12.0)
    r = self.skb.BloodSample()
    s, a, r = self.helper_bio_sample_loader(r, ocome, sstatus, ntext,
                                            lab_lab, bcode, ivol, cvol)
    #-
    vid = r.id
    r = self.skb.save(r)
    try:
      self.assertTrue(isinstance(r, self.skb.BioSample))
      self.helper_bio_sample_checker(r, vid, ocome, a, ntext,
                                     lab_lab, bcode, ivol, cvol)
    finally:
      self.skb.delete(r)
      self.skb.delete(a)
      self.skb.delete(s)

  def test_dna_sample(self):
    #-
    ocome, ntext = self.outcome_map['PASSED'], 'this is a note'
    sstatus = self.sstatus_map['USABLE']
    lab_lab, bcode, ivol, cvol = ('lab-label', 'barcode', 12.3, 12.0)
    r = self.skb.DNASample()
    s, a, r = self.helper_bio_sample_loader(r, ocome, sstatus, ntext,
                                            lab_lab, bcode, ivol, cvol)
    nanodrop, qp230260, qp230280 = 33, 0.2, 0.44
    r.nanodropConcentration, r.qp230260, r.qp230280 = \
                             nanodrop, qp230260, qp230280
    #-
    vid = r.id
    r = self.skb.save(r)
    try:
      self.assertTrue(isinstance(r, self.skb.BioSample))
      self.helper_bio_sample_checker(r, vid, ocome, a, ntext,
                                     lab_lab, bcode, ivol, cvol)
      self.assertEqual(r.nanodropConcentration, nanodrop)
      self.assertAlmostEqual(r.qp230260, qp230260, places=6)
      self.assertAlmostEqual(r.qp230280, qp230280, places=6)
    finally:
      self.skb.delete(r)
      self.skb.delete(a)
      self.skb.delete(s)

  def test_serum_sample(self):
    #-
    ocome, ntext = self.outcome_map['PASSED'], 'this is a note'
    sstatus = self.sstatus_map['USABLE']
    lab_lab, bcode, ivol, cvol = ('lab-label', 'barcode', 12.3, 12.0)
    r = self.skb.BloodSample()
    s, a, r = self.helper_bio_sample_loader(r, ocome, sstatus, ntext,
                                            lab_lab, bcode, ivol, cvol)
    #-
    vid = r.id
    r = self.skb.save(r)
    try:
      self.assertTrue(isinstance(r, self.skb.BioSample))
      self.helper_bio_sample_checker(r, vid, ocome, a, ntext,
                                     lab_lab, bcode, ivol, cvol)
    finally:
      self.skb.delete(r)
      self.skb.delete(a)
      self.skb.delete(s)

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestSKB('test_study'))
  suite.addTest(TestSKB('test_device'))
  suite.addTest(TestSKB('test_action_setup'))
  suite.addTest(TestSKB('test_action'))
  suite.addTest(TestSKB('test_result'))
  suite.addTest(TestSKB('test_sample'))
  suite.addTest(TestSKB('test_data_sample'))
  suite.addTest(TestSKB('test_bio_sample'))
  suite.addTest(TestSKB('test_blood_sample'))
  suite.addTest(TestSKB('test_dna_sample'))
  suite.addTest(TestSKB('test_serum_sample'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

