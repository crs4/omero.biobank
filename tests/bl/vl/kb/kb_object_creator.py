import os, unittest, time
import itertools as it
from bl.vl.kb import KBError
from bl.vl.kb import KnowledgeBase as KB
import bl.vl.utils as vlu

import logging
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger()

class KBObjectCreator(unittest.TestCase):
  def __init__(self, label):
    self.kb = 'THIS_IS_A_DUMMY'
    self.kill_list = 'THIS_IS_A_DUMMY'
    super(KBObjectCreator, self).__init__(label)

  def configure_object(self, o, conf):
    for k in conf.keys():
      logger.debug('o[%s] setting %s to %s' % (o.id, k, conf[k]))
      setattr(o, k, conf[k])
    conf['id'] = o.id

  def create_study(self):
    pars = {'label' : 'foobar_%f' % time.time(),
            'description' : 'this is a fake desc'}
    s = self.kb.ObjectFactory().create(self.kb.Study, pars)
    pars['id'] = s.id
    return pars, s

  def create_device(self):
    conf = {'label' : 'foo-%f' % time.time(),
            'maker' : 'foomaker',
            'model' : 'foomodel',
            'release' : '%f' % time.time(),
            'physicalLocation' : 'HERE_THERE_EVERYWHERE'}
    device = self.kb.ObjectFactory().create(self.kb.Device, pars)
    pars['id'] = device.id
    return conf, device

  def create_action_setup(self, action_setup=None):
    conf = {'label' : 'asetup-%f' % time.time(),
            'conf' : '{"param1": "foo"}'}
    action_setup = (action_setup if action_setup
                    else self.kb.ObjectFactory().create(self.kb.ActionSetup,
                                                        conf))
    conf['id'] = action_setup.id
    return conf, action_setup

  def create_action(self, action_klass=None, target=None):
    dev_conf, device = self.create_device()
    self.kill_list.append(device.save())
    #--
    asu_conf, asetup = self.create_action_setup()
    self.kill_list.append(asetup.save())
    #--
    stu_conf, study = self.create_study()
    self.kill_list.append(study.save())
    #--
    conf = {'setup' : asetup,
            'device': device,
            'actionCategory' : self.acat_map['ACQUISITION'],
            'operator' : 'Alfred E. Neumann',
            'context'  : study,
            'description' : 'description ...',
            'target' : target
            }
    action_klass = action_klass if action_klass else self.kb.Action
    action = action if action else self.kb.ObjectFactory().create(action_klass,
                                                                  conf)
    return conf, action

  def create_action_on_vessel(self, vessel=None):
    if not vessel:
      vconf, vessel = self.create_vessel()
      self.kill_list.append(vessel.save())
    return self.create_action(action_klass=self.kb.ActionOnVessel,
                              target=vessel)

  def create_action_on_data_sample(self, data_sample=None):
    if not data_sample:
      vconf, data_sample = self.create_data_sample()
      self.kill_list.append(data_sample.save())
    return self.create_action(action_klass=self.kb.ActionOnVessel,
                              target=data_sample)

  def create_action_on_data_collection_item(self, dc_item=None):
    if not dc_item:
      dcconf, dc_item = self.create_data_collection_item()
      self.kill_list.append(dc_item.save())
    return self.create_action(action_klass=self.kb.ActionOnDataCollectionItem,
                              target=dc_item)

  def create_action_on_action(self, action=None):
    if not action:
      aconf, action = self.create_action()
      self.kill_list.append(action.save())
    return self.create_action(action_klass=self.kb.ActionOnAction,
                              target=action)

  def create_result(self, result=None, action=None):
    result = result if result is not None else self.kb.Result()
    #-
    conf, study = self.create_study()
    study = self.kb.save(study)
    self.kill_list.append(study)
    #-
    if action is None:
      conf, action = self.create_action()
      action = self.kb.save(action)
      self.kill_list.append(action)
    #-
    conf = {'action' : action,
            'outcome' : self.outcome_map['OK'],
            'notes'  : 'this is a note'}
    self.configure_object(result, conf)
    return conf, result

  def create_sample(self, sample=None, action=None):
    label = 'sample-label-%f' % time.time()
    sample = sample if sample else self.kb.Sample(label=label)
    return self.create_result(result=sample, action=action)

  def create_data_sample(self, action=None):
    label = 'data-sample-label-%f' % time.time()
    dtype = self.dtype_map['GTRAW']
    conf, sample = self.create_sample(sample=self.kb.DataSample(label=label,
                                                                 data_type= dtype),
                                      action=action)
    conf['label'] = label
    conf['dataType'] = dtype
    return conf, sample

  def create_affymetrix_cel(self, action=None):
    label = 'affymetrix-cel-label-%f' % time.time()
    dtype = self.dtype_map['GTRAW']
    array_type = 'GenomeWideSNP_6'
    sample = self.kb.AffymetrixCel(label=label,
                                    array_type=array_type,
                                    data_type= dtype)
    conf, sample = self.create_sample(sample=sample,
                                      action=action)
    conf['label'] = label
    conf['arrayType'] = array_type
    conf['dataType'] = dtype
    return conf, sample

  def create_snp_markers_set(self, action=None):
    conf = {'maker' : 'snp-foomaker',
            'model' : 'snp-foomodel',
            'release' : 'snp-rel-%f' % time.time(),
            'markersSetVID' : vlu.make_vid()}
    result = self.kb.SNPMarkersSet(maker=conf['maker'], model=conf['model'], release=conf['release'],
                                    set_vid=conf['markersSetVID'])
    sconf, res = self.create_result(result=result, action=action)
    conf.update(sconf)
    return conf, res


  def create_genotype_data_sample(self, action=None):
    conf, markers_set = self.create_snp_markers_set()
    markers_set = self.kb.save(markers_set)
    self.kill_list.append(markers_set)
    label = 'genotype-data-sample-label-%f' % time.time()
    dtype = self.dtype_map['GTCALL']
    conf, sample = self.create_sample(sample=self.kb.GenotypeDataSample(label=label,
                                                                         snp_markers_set=markers_set,
                                                                         data_type= dtype),
                                      action=action)
    conf['label'] = label
    conf['dataType'] = dtype
    return conf, sample


  def create_data_object(self, data_sample=None, action=None):
    if not data_sample:
      conf, data_sample = self.create_data_sample(action=action)
      data_sample = self.kb.save(data_sample)
      self.kill_list.append(data_sample)

    conf = {'sample' : data_sample,
            'mimetype' : 'x-application/foo',
            'path'     : 'file:/usr/share/%s/foo.dat' % time.time(),
            'sha1'     : 'this should be a sha1',
            'size'     :  100
            }
    do = self.kb.DataObject(sample=conf['sample'],
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
      sample = self.kb.BioSample(label=sconf['label'])
    conf, bio_sample = self.create_sample(sample=sample, action=action)
    self.configure_object(sample, sconf)
    conf.update(sconf)
    return conf, sample

  def create_blood_sample(self, action=None):
    label = 'blood-sample-label-%f' % time.time()
    return self.create_bio_sample(sample=self.kb.BloodSample(label=label),
                                  action=action)

  def create_dna_sample(self, action=None):
    label = 'dna-sample-label-%f' % time.time()
    sconf = {'label' : label,
             'nanodropConcentration' : 33,
             'qp230260'  : 0.33,
             'qp230280'  : 0.44}
    conf, dna_sample = self.create_bio_sample(sample=self.kb.DNASample(label=label),
                                              action=action)
    self.configure_object(dna_sample, sconf)
    conf.update(sconf)
    return conf, dna_sample

  def create_serum_sample(self, action=None):
    label = 'serum-sample-label-%f' % time.time()
    return self.create_bio_sample(sample=self.kb.SerumSample(label=label),
                                  action=action)

  def create_sample_chain(self, root_action=None):
    conf, blood_sample = self.create_blood_sample(action=root_action)
    blood_sample = self.kb.save(blood_sample)
    self.kill_list.append(blood_sample)
    #-
    conf, action = self.create_action_on_sample(sample=blood_sample)
    action = self.kb.save(action)
    self.kill_list.append(action)
    #-
    conf, dna_sample = self.create_dna_sample(action=action)
    dna_sample = self.kb.save(dna_sample)
    self.kill_list.append(dna_sample)
    #-
    conf, action2 = self.create_action_on_sample(sample=dna_sample)
    action2 = self.kb.save(action2)
    self.kill_list.append(action2)
    #-
    conf, data_sample = self.create_data_sample(action=action2)
    return conf, data_sample

  def create_samples_container(self, result=None):
    sconf = {'label' : 'sc-lab_label-%f' % time.time(),
             'barcode'  : 'sc-barcode-%s' % time.time(),
             'virtualContainer' : False,
             'slots' : 96}
    result = result if result is not None \
                    else self.kb.SamplesContainer(slots=sconf['slots'],
                                                   barcode=sconf['barcode'])
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
    titer_plate = self.kb.TiterPlate(rows=sconf['rows'],
                                      columns=sconf['columns'],
                                      barcode=sconf['barcode'],
                                      virtual_container=sconf['virtualContainer'])
    conf, titer_plate = self.create_result(result=titer_plate)
    self.configure_object(titer_plate, sconf)
    conf.update(sconf)
    return conf, titer_plate

  def create_action_on_container(self):
    conf, target = self.create_samples_container()
    target = self.kb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.kb.ActionOnSamplesContainer(), target=target)
    return conf, action

  def create_container_slot(self, container=None):
    conf, sample = self.create_bio_sample()
    sample = self.kb.save(sample)
    self.kill_list.append(sample)
    #-
    conf, container = self.create_samples_container()
    container = self.kb.save(container)
    self.kill_list.append(container)
    #-
    sconf = { 'sample' : sample, 'container' : container, 'slotPosition' : 3}
    container_slot = self.kb.SamplesContainerSlot(sample=sconf['sample'],
                                                   container=sconf['container'],
                                                   slot=sconf['slotPosition'])
    conf, container_slot = self.create_result(result=container_slot)
    conf.update(sconf)
    conf['id'] = container_slot.id
    return conf, container_slot

  def create_plate_well(self, sample=None, container=None, label=None, row=1, column=2):
    if sample is None:
      conf, sample = self.create_bio_sample()
      sample = self.kb.save(sample)
      self.kill_list.append(sample)
    #-
    if container is None:
      conf, container = self.create_titer_plate()
      container = self.kb.save(container)
      self.kill_list.append(container)
    #-
    if label is None:
      label = container.label + '.%03d%03d' % (row, column)
    sconf = {'label' : label, 'sample' : sample, 'container' : container,
             'row' : row, 'column' : column, 'volume' : 0.23}
    container_slot = self.kb.PlateWell(sample=sconf['sample'],
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
    target = self.kb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.kb.ActionOnSamplesContainerSlot(), target=target)
    return conf, action

  def create_data_collection(self):
    conf, study = self.create_study()
    study = self.kb.save(study)
    self.kill_list.append(study)
    conf = {'description' : 'this is a fake description',
            'label' : 'a-dc-label-%s' % time.time(),
            'study' : study}
    data_collection = self.kb.DataCollection(study=study,
                                              label=conf['label'])
    self.configure_object(data_collection, conf)
    return conf, data_collection

  def create_data_collection_item(self):
    conf, data_collection = self.create_data_collection()
    data_collection = self.kb.save(data_collection)
    self.kill_list.append(data_collection)
    #-
    conf, sample = self.create_data_sample()
    sample = self.kb.save(sample)
    self.kill_list.append(sample)
    #-
    conf = {'dataSample' : sample, 'dataSet' : data_collection}
    item = self.kb.DataCollectionItem(data_sample=conf['dataSample'],
                                       data_collection= conf['dataSet'])
    sconf, item = self.create_result(result=item)
    conf['id'] = item.id
    conf.update(sconf)
    return conf, item

  def create_action_on_data_collection(self):
    conf, target = self.create_data_collection()
    target = self.kb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.kb.ActionOnDataCollection(),
                                      target=target)
    return conf, action

  def create_action_on_data_collection_item(self):
    conf, target = self.create_data_collection_item()
    target = self.kb.save(target)
    self.kill_list.append(target)
    conf, action = self.create_action(action=self.kb.ActionOnDataCollectionItem(),
                                      target=target)
    return conf, action
