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
  def __init__(self, name):
    self.kill_list = []
    super(TestSKB, self).__init__(name)

  def setUp(self):
    self.skb = sKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)
    self.atype_map   = self.skb.get_action_type_table()
    self.outcome_map = self.skb.get_result_outcome_table()
    self.sstatus_map = self.skb.get_sample_status_table()

  def tearDown(self):
    self.kill_list.reverse()
    for x in self.kill_list:
      #print 'deleting %s[%s]' % (type(x), x.id)
      self.skb.delete(x)
    self.kill_list = []

  def configure_object(self, o, conf):
    for k in conf.keys():
      setattr(o, k, conf[k])
    conf['id'] = o.id

  def create_study(self):
    pars = {'label' : 'foobar_%f' % time.time()}
    s = self.skb.Study(label=pars['label'])
    pars['id'] = s.id
    return pars, s

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

  def create_device(self, device = None):
    device = device if device else self.skb.Device()
    conf = {'vendor' : 'foomaker', 'model' : 'foomodel', 'release' : '0.2'}
    self.configure_object(device, conf)
    return conf, device

  def test_device(self):
    conf, d = self.create_device()
    d = self.skb.save(d)
    self.check_object(d, conf, self.skb.Device)
    self.skb.delete(d)

  def create_action_setup(self, action_setup=None):
    action_setup = action_setup if action_setup else self.skb.ActionSetup()
    conf = {'notes' : 'hooo'}
    action_setup.notes = conf['notes']
    conf['id'] = action_setup.id
    return conf, action_setup

  def test_action_setup(self):
    conf, a = self.create_action_setup()
    a = self.skb.save(a)
    self.check_object(a, conf, self.skb.ActionSetup)
    self.skb.delete(a)

  def create_action(self, action=None):
    action = action if action else self.skb.Action()
    dev_conf, device = self.create_device()
    device = self.skb.save(device)
    self.kill_list.append(device)
    #--
    asu_conf, asetup = self.create_action_setup()
    asetup = self.skb.save(asetup)
    self.kill_list.append(asetup)
    #--
    stu_conf, study = self.create_study()
    study = self.skb.save(study)
    self.kill_list.append(study)
    #--
    conf = {'setup' : asetup,
            'device': device,
            'actionType' : self.atype_map['ACQUISITION'],
            'operator' : 'Alfred E. Neumann',
            'context'  : study,
            'description' : 'description ...'}
    self.configure_object(action, conf)
    return conf, action

  def test_action(self):
    conf, action = self.create_action()
    action = self.skb.save(action)
    self.check_object(action, conf, self.skb.Action)
    self.skb.delete(action)

  #---
  def create_result(self, result=None):
    result = result if result else self.skb.Result()
    #-
    conf, study = self.create_study()
    study = self.skb.save(study)
    self.kill_list.append(study)
    #-
    conf, action = self.create_action()
    action = self.skb.save(action)
    self.kill_list.append(action)
    #-
    conf = {'action' : action, 'outcome' : self.outcome_map['PASSED'],
            'notes'  : 'this is a note'}
    self.configure_object(result, conf)
    return conf, result

  def test_result(self):
    conf, result = self.create_result()
    result = self.skb.save(result)
    self.check_object(result, conf, self.skb.Result)
    self.skb.delete(result)

  def create_sample(self, sample=None):
    sample = sample if sample else self.skb.Sample()
    return self.create_result(result=sample)

  def test_sample(self):
    conf, sample = self.create_sample()
    sample = self.skb.save(sample)
    self.check_object(sample, conf, self.skb.Sample)
    self.skb.delete(sample)

  def create_data_sample(self):
    name = 'data-sample-name-%f' % time.time()
    conf, sample = self.create_sample(sample=self.skb.DataSample(name=name))
    conf['name'] = name
    return conf, sample

  def test_data_sample(self):
    conf, data_sample = self.create_data_sample()
    data_sample = self.skb.save(data_sample)
    self.check_object(data_sample, conf, self.skb.DataSample)
    self.skb.delete(data_sample)

  def create_bio_sample(self, bio_sample=None):
    sconf = {'labLabel' : 'bio-sample-lab-label-%f' % time.time(),
             'barcode'  : 'bio-sample-barcode-%f' % time.time(),
             'initialVolume' : 1.0,
             'currentVolume' : 0.8,
             'status' : self.sstatus_map['USABLE']}
    if bio_sample is None:
      bio_sample = self.skb.BioSample()
    conf, bio_sample = self.create_sample(sample=bio_sample)
    self.configure_object(bio_sample, sconf)
    conf.update(sconf)
    return conf, bio_sample

  def test_bio_sample(self):
    conf, bio_sample = self.create_bio_sample()
    bio_sample = self.skb.save(bio_sample)
    self.check_object(bio_sample, conf, self.skb.BioSample)
    self.skb.delete(bio_sample)

  def create_blood_sample(self):
    return self.create_bio_sample(self.skb.BloodSample())

  def test_blood_sample(self):
    conf, blood_sample = self.create_blood_sample()
    blood_sample = self.skb.save(blood_sample)
    self.check_object(blood_sample, conf, self.skb.BloodSample)
    self.skb.delete(blood_sample)

  def create_dna_sample(self):
    sconf = {'nanodropConcentration' : 33,
             'qp230260'  : 0.33,
             'qp230280'  : 0.44}
    conf, dna_sample = self.create_bio_sample(self.skb.DNASample())
    self.configure_object(dna_sample, sconf)
    conf.update(sconf)
    return conf, dna_sample

  def test_dna_sample(self):
    conf, dna_sample = self.create_dna_sample()
    dna_sample = self.skb.save(dna_sample)
    self.check_object(dna_sample, conf, self.skb.DNASample)
    self.skb.delete(dna_sample)

  def create_serum_sample(self):
    return self.create_bio_sample(self.skb.SerumSample())

  def test_serum_sample(self):
    conf, serum_sample = self.create_serum_sample()
    serum_sample = self.skb.save(serum_sample)
    self.check_object(serum_sample, conf, self.skb.SerumSample)
    self.skb.delete(serum_sample)

  #---
  def create_samples_container(self, sc=None):
    conf = {'labLabel' : 'sc-lab_label-%f' % time.time(),
            'barcode'  : 'sc-barcode-%s' % time.time(),
            'virtualContainer' : False,
            'slots' : 96}
    if sc is None:
      sc = self.skb.SamplesContainer(slots=conf['slots'])
    else:
      conf['slots'] = sc.slots
    self.configure_object(sc, conf)
    return conf, sc

  def test_samples_container(self):
    conf, sc = self.create_samples_container()
    sc = self.skb.save(sc)
    self.check_object(sc, conf, self.skb.SamplesContainer)
    self.skb.delete(sc)

  def create_titer_plate(self, titer_plate=None):
    if titer_plate is None:
      n_rows, n_columns = 16, 16
      titer_plate = self.skb.TiterPlate(rows=n_rows,
                                        columns=n_columns)
    else:
      n_rows, n_columns = titer_plate.rows, titer_plate.columns
    conf, tp = self.create_samples_container(titer_plate)
    conf['rows'], conf['columns'] = n_rows, n_columns
    titer_plate.rows = conf['rows']
    titer_plate.columns = conf['columns']
    return conf, titer_plate

  def test_titer_plate(self):
    conf, tp = self.create_titer_plate()
    tp = self.skb.save(tp)
    self.check_object(tp, conf, self.skb.TiterPlate)
    self.skb.delete(tp)

  def create_action_on_sample(self):
    conf, sample = self.create_sample()
    sample = self.skb.save(sample)
    self.kill_list.append(sample)
    #--
    conf, action = self.create_action(action=self.skb.ActionOnSample())
    sconf = { 'target' : sample}
    self.configure_object(action, sconf)
    conf.update(sconf)
    return conf, action

  def test_action_on_sample(self):
    conf, action = self.create_action_on_sample()
    action = self.skb.save(action)
    self.check_object(action, conf, self.skb.ActionOnSample)
    self.skb.delete(action)

  def create_action_on_container(self):
    conf, target = self.create_samples_container()
    target = self.skb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.skb.ActionOnContainer())
    sconf = {'target' : target}
    self.configure_object(action, sconf)
    conf.update(sconf)
    return conf, action

  def test_action_on_container(self):
    conf, action = self.create_action_on_container()
    action = self.skb.save(action)
    self.check_object(action, conf, self.skb.ActionOnContainer)
    self.skb.delete(action)

  def create_container_slot(self, container=None):
    conf, sample = self.create_bio_sample()
    sample = self.skb.save(sample)
    self.kill_list.append(sample)
    #-
    conf, container = self.create_samples_container()
    container = self.skb.save(container)
    self.kill_list.append(container)
    #-
    sconf = { 'sample' : sample, 'container' : container, 'slotPosition' : 3}
    container_slot = self.skb.ContainerSlot(sample=sconf['sample'],
                                            container=sconf['container'],
                                            slot_position=sconf['slotPosition'])
    conf, container_slot = self.create_result(result=container_slot)
    conf.update(sconf)
    conf['id'] = container_slot.id
    return conf, container_slot

  def test_container_slot(self):
    conf, container_slot = self.create_container_slot()
    container_slot = self.skb.save(container_slot)
    self.check_object(container_slot, conf, self.skb.ContainerSlot)
    self.skb.delete(container_slot)

  def create_plate_well(self):
    conf, sample = self.create_bio_sample()
    sample = self.skb.save(sample)
    self.kill_list.append(sample)
    #-
    conf, container = self.create_titer_plate()
    container = self.skb.save(container)
    self.kill_list.append(container)
    #-
    sconf = { 'sample' : sample, 'container' : container,
              'row' : 3, 'column' : 4, 'volume' : 0.23}
    container_slot = self.skb.PlateWell(sample=sconf['sample'],
                                        container=sconf['container'],
                                        row=sconf['row'],
                                        column=sconf['column'],
                                        volume=sconf['volume'])
    conf, container_slot = self.create_result(result=container_slot)
    conf.update(sconf)
    conf['id'] = container_slot.id
    return conf, container_slot

  def test_plate_well(self):
    conf, plate_well = self.create_plate_well()
    plate_well = self.skb.save(plate_well)
    self.check_object(plate_well, conf, self.skb.PlateWell)
    self.skb.delete(plate_well)

  def create_action_on_sample_slot(self):
    conf, target = self.create_container_slot()
    target = self.skb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.skb.ActionOnSampleSlot())
    sconf = { 'target' : target}
    self.configure_object(action, sconf)
    conf.update(sconf)
    return conf, action

  def test_action_on_sample_slot(self):
    conf, action = self.create_action_on_sample_slot()
    action = self.skb.save(action)
    self.check_object(action, conf, self.skb.ActionOnSampleSlot)
    self.skb.delete(action)

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
  suite.addTest(TestSKB('test_samples_container'))
  suite.addTest(TestSKB('test_titer_plate'))
  suite.addTest(TestSKB('test_container_slot'))
  suite.addTest(TestSKB('test_plate_well'))
  suite.addTest(TestSKB('test_action_on_sample'))
  suite.addTest(TestSKB('test_action_on_container'))
  suite.addTest(TestSKB('test_action_on_sample_slot'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

