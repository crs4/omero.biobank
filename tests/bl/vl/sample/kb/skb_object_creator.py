import os, unittest, time
import itertools as it
from bl.lib.sample.kb import KBError
from bl.lib.sample.kb import KnowledgeBase as sKB

import logging
logging.basicConfig(level=logging.DEBUG)


class SKBObjectCreator(unittest.TestCase):
  def __init__(self, name):
    self.skb = 'THIS_IS_A_DUMMY'
    self.kill_list = 'THIS_IS_A_DUMMY'
    super(SKBObjectCreator, self).__init__(name)

  def configure_object(self, o, conf):
    for k in conf.keys():
      setattr(o, k, conf[k])
    conf['id'] = o.id

  def create_data_object(self):
    conf = {'name' : 'foobar_%f' % time.time(),
            'mimetype' : 'x-application/foo',
            'path'     : 'file:/usr/share/foo.dat',
            'sha1'     : 'this should be a sha1',
            'size'     :  100
            }
    do = self.skb.DataObject(name=conf['name'],
                             mime_type=conf['mimetype'],
                             path=conf['path'],
                             sha1=conf['sha1'],
                             size=conf['size'])
    return conf, do

  def create_study(self):
    pars = {'label' : 'foobar_%f' % time.time()}
    s = self.skb.Study(label=pars['label'])
    pars['id'] = s.id
    return pars, s

  def create_device(self):
    conf = {'vendor' : 'foomaker',
            'model' : 'foomodel',
            'release' : '%f' % time.time()}
    device = self.skb.Device(vendor=conf['vendor'],
                             model=conf['model'],
                             release=conf['release'])
    self.configure_object(device, conf)
    return conf, device

  def create_action_setup(self, action_setup=None):
    conf = {'label' : 'asetup-%f' % time.time(),
            'conf' : '{"param1": "foo"}'}
    action_setup = action_setup if action_setup \
                                else self.skb.ActionSetup(label=conf['label'])
    action_setup.conf = conf['conf']
    conf['id'] = action_setup.id
    return conf, action_setup

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

  def create_sample(self, sample=None):
    sample = sample if sample else self.skb.Sample()
    return self.create_result(result=sample)

  def create_data_sample(self):
    name = 'data-sample-name-%f' % time.time()
    dtype = self.dtype_map['GTRAW']
    conf, sample = self.create_sample(sample=self.skb.DataSample(name=name, data_type= dtype))
    conf['name'] = name
    conf['dataType'] = dtype
    return conf, sample

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

  def create_blood_sample(self):
    return self.create_bio_sample(self.skb.BloodSample())

  def create_dna_sample(self):
    sconf = {'nanodropConcentration' : 33,
             'qp230260'  : 0.33,
             'qp230280'  : 0.44}
    conf, dna_sample = self.create_bio_sample(self.skb.DNASample())
    self.configure_object(dna_sample, sconf)
    conf.update(sconf)
    return conf, dna_sample

  def create_serum_sample(self):
    return self.create_bio_sample(self.skb.SerumSample())

  def create_sample_chain(self):
    conf, blood_sample = self.create_blood_sample()
    blood_sample = self.skb.save(blood_sample)
    self.kill_list.append(blood_sample)
    #-
    conf, action = self.create_action_on_sample()
    action.target = blood_sample
    action = self.skb.save(action)
    self.kill_list.append(action)
    #-
    conf, dna_sample = self.create_dna_sample()
    dna_sample.action = action
    dna_sample = self.skb.save(dna_sample)
    self.kill_list.append(dna_sample)
    #-
    conf, action2 = self.create_action_on_sample()
    action2.target = dna_sample
    action2 = self.skb.save(action2)
    self.kill_list.append(action2)
    #-
    conf, data_sample = self.create_data_sample()
    data_sample.action = action2
    return conf, data_sample

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

  def create_titer_plate(self):
    conf = {'labLabel' : 'tp-lab_label-%f' % time.time(),
            'barcode'  : 'tp-barcode-%s' % time.time(),
            'virtualContainer' : False,
            'rows' : 4,
            'columns' : 4}
    titer_plate = self.skb.TiterPlate(rows=conf['rows'],
                                      columns=conf['columns'],
                                      barcode=conf['barcode'],
                                      virtual_container=conf['virtualContainer'])
    return conf, titer_plate

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

  def create_action_on_container(self):
    conf, target = self.create_samples_container()
    target = self.skb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.skb.ActionOnContainer())
    sconf = {'target' : target}
    self.configure_object(action, sconf)
    conf.update(sconf)
    return conf, action

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

  def create_plate_well(self, sample=None, container=None, row=1, column=2):
    if sample is None:
      conf, sample = self.create_bio_sample()
      sample = self.skb.save(sample)
      self.kill_list.append(sample)
    #-
    if container is None:
      conf, container = self.create_titer_plate()
      container = self.skb.save(container)
      self.kill_list.append(container)
    #-
    sconf = { 'sample' : sample, 'container' : container,
              'row' : row, 'column' : column, 'volume' : 0.23}
    container_slot = self.skb.PlateWell(sample=sconf['sample'],
                                        container=sconf['container'],
                                        row=sconf['row'],
                                        column=sconf['column'],
                                        volume=sconf['volume'])
    conf, container_slot = self.create_result(result=container_slot)
    conf.update(sconf)
    conf['id'] = container_slot.id
    return conf, container_slot

  def create_action_on_sample_slot(self):
    conf, target = self.create_container_slot()
    target = self.skb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.skb.ActionOnSampleSlot())
    sconf = { 'target' : target}
    self.configure_object(action, sconf)
    conf.update(sconf)
    return conf, action

  def create_data_collection(self):
    conf, study = self.create_study()
    study = self.skb.save(study)
    self.kill_list.append(study)
    conf = {'description' : 'this is a fake description',
            'study' : study}
    data_collection = self.skb.DataCollection(study=study)
    self.configure_object(data_collection, conf)
    return conf, data_collection

  def create_data_collection_item(self):
    conf, data_collection = self.create_data_collection()
    data_collection = self.skb.save(data_collection)
    self.kill_list.append(data_collection)
    #-
    conf, sample = self.create_data_sample()
    sample = self.skb.save(sample)
    self.kill_list.append(sample)
    #-
    conf = {'dataSample' : sample, 'dataSet' : data_collection}
    item = self.skb.DataCollectionItem(data_sample=conf['dataSample'],
                                       data_collection= conf['dataSet'])
    self.configure_object(item, conf)
    return conf, item

  def create_action_on_data_collection(self):
    conf, target = self.create_data_collection()
    target = self.skb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.skb.ActionOnDataCollection())
    sconf = {'target' : target}
    self.configure_object(action, sconf)
    conf.update(sconf)
    return conf, action

  def create_action_on_data_collection_item(self):
    conf, target = self.create_data_collection_item()
    target = self.skb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.skb.ActionOnDataCollectionItem())
    sconf = {'target' : target}
    self.configure_object(action, sconf)
    conf.update(sconf)
    return conf, action
