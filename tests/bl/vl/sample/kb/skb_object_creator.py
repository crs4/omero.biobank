import os, unittest, time
import itertools as it
from bl.vl.sample.kb import KBError
from bl.vl.sample.kb import KnowledgeBase as sKB
import bl.vl.utils           as vlu

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

class SKBObjectCreator(unittest.TestCase):
  def __init__(self, name):
    self.skb = 'THIS_IS_A_DUMMY'
    self.kill_list = 'THIS_IS_A_DUMMY'
    super(SKBObjectCreator, self).__init__(name)

  def configure_object(self, o, conf):
    assert hasattr(o, "ome_obj")
    for k in conf.keys():
      logger.debug('o[%s] setting %s to %s' % (o.id, k, conf[k]))
      setattr(o, k, conf[k])
    conf['id'] = o.id

  def create_study(self):
    pars = {'label' : 'foobar_%f' % time.time()}
    s = self.skb.Study(label=pars['label'])
    pars['id'] = s.id
    return pars, s

  def create_device(self):
    conf = {'label' : 'foo-%f' % time.time(),
            'maker' : 'foomaker',
            'model' : 'foomodel',
            'release' : '%f' % time.time()}
    device = self.skb.Device(label=conf['label'],
                             maker=conf['maker'],
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

  def create_action(self, action=None, target=None):
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
            'actionCategory' : self.acat_map['ACQUISITION'],
            'operator' : 'Alfred E. Neumann',
            'context'  : study,
            'description' : 'description ...'
            }
    self.configure_object(action, conf)
    if target:
      conf['target'] = target
      action.target = target
    return conf, action

  def create_action_on_data_collection(self, collection=None):
    return self.create_action(action=self.skb.ActionOnDataCollection(),
                              target=collection)

  def create_action_on_data_collection_item(self, item=None):
    return self.create_action(action=self.skb.ActionOnDataCollectionItem(),
                              target=item)

  def create_action_on_sample(self, sample=None):
    return self.create_action(action=self.skb.ActionOnSample(),
                              target=sample)

  def create_action_on_samples_container(self, container=None):
    return self.create_action(action=self.skb.ActionOnSamplesContainer(),
                              target=container)

  def create_action_on_samples_container_slot(self, slot=None):
    return self.create_action(action=self.skb.ActionOnSamplesContainerSlot(),
                              target=slot)

  def create_result(self, result=None, action=None):
    result = result if result is not None else self.skb.Result()
    #-
    conf, study = self.create_study()
    study = self.skb.save(study)
    self.kill_list.append(study)
    #-
    if action is None:
      conf, action = self.create_action()
      action = self.skb.save(action)
      self.kill_list.append(action)
    #-
    conf = {'action' : action,
            'outcome' : self.outcome_map['OK'],
            'notes'  : 'this is a note'}
    self.configure_object(result, conf)
    return conf, result

  def create_sample(self, sample=None, action=None):
    sample = sample if sample else self.skb.Sample()
    return self.create_result(result=sample, action=action)

  def create_data_sample(self, action=None):
    name = 'data-sample-name-%f' % time.time()
    dtype = self.dtype_map['GTRAW']
    conf, sample = self.create_sample(sample=self.skb.DataSample(name=name, data_type= dtype),
                                      action=action)
    conf['name'] = name
    conf['dataType'] = dtype
    return conf, sample

  def create_affymetrix_cel(self, action=None):
    name = 'affymetrix-cel-name-%f' % time.time()
    dtype = self.dtype_map['GTRAW']
    array_type = 'GenomeWideSNP_6'
    sample = self.skb.AffymetrixCel(name=name,
                                    array_type=array_type,
                                    data_type= dtype)
    conf, sample = self.create_sample(sample=sample,
                                      action=action)
    conf['name'] = name
    conf['arrayType'] = array_type
    conf['dataType'] = dtype
    return conf, sample

  def create_snp_markers_set(self, action=None):
    conf = {'maker' : 'snp-foomaker',
            'model' : 'snp-foomodel',
            'release' : 'snp-rel-%f' % time.time(),
            'markersSetVID' : vlu.make_vid()}
    result = self.skb.SNPMarkersSet(maker=conf['maker'], model=conf['model'], release=conf['release'],
                                    set_vid=conf['markersSetVID'])
    sconf, res = self.create_result(result=result, action=action)
    conf.update(sconf)
    return conf, res


  def create_genotype_data_sample(self, action=None):
    conf, markers_set = self.create_snp_markers_set()
    markers_set = self.skb.save(markers_set)
    self.kill_list.append(markers_set)
    name = 'genotype-data-sample-name-%f' % time.time()
    dtype = self.dtype_map['GTCALL']
    conf, sample = self.create_sample(sample=self.skb.GenotypeDataSample(name=name,
                                                                         snp_markers_set=markers_set,
                                                                         data_type= dtype),
                                      action=action)
    conf['name'] = name
    conf['dataType'] = dtype
    return conf, sample


  def create_data_object(self, action=None):
    conf, data_sample = self.create_data_sample(action=action)
    data_sample = self.skb.save(data_sample)
    self.kill_list.append(data_sample)

    conf = {'sample' : data_sample,
            'mimetype' : 'x-application/foo',
            'path'     : 'file:/usr/share/foo.dat',
            'sha1'     : 'this should be a sha1',
            'size'     :  100
            }
    do = self.skb.DataObject(sample=conf['sample'],
                             mime_type=conf['mimetype'],
                             path=conf['path'],
                             sha1=conf['sha1'],
                             size=conf['size'])
    return conf, do

  def create_bio_sample(self, sample=None, action=None):
    sconf = {'label' : 'bio-sample-lab-label-%f' % time.time(),
             'barcode'  : 'bio-sample-barcode-%f' % time.time(),
             'initialVolume' : 1.0,
             'currentVolume' : 0.8,
             'status' : self.sstatus_map['USABLE']}
    if sample is None:
      sample = self.skb.BioSample()
    conf, bio_sample = self.create_sample(sample=sample, action=action)
    self.configure_object(sample, sconf)
    conf.update(sconf)
    return conf, sample

  def create_blood_sample(self, action=None):
    return self.create_bio_sample(sample=self.skb.BloodSample(), action=action)

  def create_dna_sample(self, action=None):
    sconf = {'nanodropConcentration' : 33,
             'qp230260'  : 0.33,
             'qp230280'  : 0.44}
    conf, dna_sample = self.create_bio_sample(sample=self.skb.DNASample(), action=action)
    self.configure_object(dna_sample, sconf)
    conf.update(sconf)
    return conf, dna_sample

  def create_serum_sample(self, action=None):
    return self.create_bio_sample(sample=self.skb.SerumSample(), action=action)

  def create_sample_chain(self, root_action=None):
    conf, blood_sample = self.create_blood_sample(action=root_action)
    blood_sample = self.skb.save(blood_sample)
    self.kill_list.append(blood_sample)
    #-
    conf, action = self.create_action_on_sample(sample=blood_sample)
    action = self.skb.save(action)
    self.kill_list.append(action)
    #-
    conf, dna_sample = self.create_dna_sample(action=action)
    dna_sample = self.skb.save(dna_sample)
    self.kill_list.append(dna_sample)
    #-
    conf, action2 = self.create_action_on_sample(sample=dna_sample)
    action2 = self.skb.save(action2)
    self.kill_list.append(action2)
    #-
    conf, data_sample = self.create_data_sample(action=action2)
    return conf, data_sample

  def create_samples_container(self, result=None):
    sconf = {'label' : 'sc-lab_label-%f' % time.time(),
             'barcode'  : 'sc-barcode-%s' % time.time(),
             'virtualContainer' : False,
             'slots' : 96}

    result = result if result is not None else self.skb.SamplesContainer(slots=sconf['slots'])
    conf, container = self.create_result(result=result)
    sconf['slots'] = result.slots
    self.configure_object(container, sconf)
    conf.update(sconf)
    return conf, container

  def create_titer_plate(self):
    sconf = {'label' : 'tp-lab_label-%f' % time.time(),
            'barcode'  : 'tp-barcode-%s' % time.time(),
            'virtualContainer' : False,
            'rows' : 4,
            'columns' : 4}
    titer_plate = self.skb.TiterPlate(rows=sconf['rows'],
                                      columns=sconf['columns'],
                                      barcode=sconf['barcode'],
                                      virtual_container=sconf['virtualContainer'])
    conf, titer_plate = self.create_result(result=titer_plate)
    self.configure_object(titer_plate, sconf)
    conf.update(sconf)
    return conf, titer_plate

  def create_action_on_container(self):
    conf, target = self.create_samples_container()
    target = self.skb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.skb.ActionOnSamplesContainer(), target=target)
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
    container_slot = self.skb.SamplesContainerSlot(sample=sconf['sample'],
                                                   container=sconf['container'],
                                                   slot=sconf['slotPosition'])
    conf, container_slot = self.create_result(result=container_slot)
    conf.update(sconf)
    conf['id'] = container_slot.id
    return conf, container_slot

  def create_plate_well(self, sample=None, container=None, label=None, row=1, column=2):
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
    if label is None:
      label = container.label + '.%03d%03d' % (row, column)
    sconf = {'label' : label, 'sample' : sample, 'container' : container,
             'row' : row, 'column' : column, 'volume' : 0.23}
    container_slot = self.skb.PlateWell(sample=sconf['sample'],
                                        container=sconf['container'],
                                        row=sconf['row'],
                                        column=sconf['column'],
                                        volume=sconf['volume'])
    container_slot.label = sconf['label']
    conf, container_slot = self.create_result(result=container_slot)
    conf.update(sconf)
    conf['id'] = container_slot.id
    return conf, container_slot

  def create_action_on_container_slot(self):
    conf, target = self.create_container_slot()
    target = self.skb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.skb.ActionOnSamplesContainerSlot(), target=target)
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
    sconf, item = self.create_result(result=item)
    conf['id'] = item.id
    conf.update(sconf)
    return conf, item

  def create_action_on_data_collection(self):
    conf, target = self.create_data_collection()
    target = self.skb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.skb.ActionOnDataCollection(),
                                      target=target)
    return conf, action

  def create_action_on_data_collection_item(self):
    conf, target = self.create_data_collection_item()
    target = self.skb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.skb.ActionOnDataCollectionItem(),
                                      target=target)
    return conf, action
