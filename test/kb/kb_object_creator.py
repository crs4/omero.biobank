# BEGIN_COPYRIGHT
# END_COPYRIGHT

# pylint: disable=E1101

import unittest, time


class KBObjectCreator(unittest.TestCase):

  def __init__(self, label):
    self.kb = 'THIS_IS_A_DUMMY'
    self.kill_list = 'THIS_IS_A_DUMMY'
    super(KBObjectCreator, self).__init__(label)

  def configure_object(self, o, conf):
    for k in conf.keys():
      setattr(o, k, conf[k])
    conf['id'] = o.id

  def create_study(self):
    pars = {'label' : 'foobar_%f' % time.time(),
            'description' : 'this is a fake desc'}
    s = self.kb.factory.create(self.kb.Study, pars)
    pars['id'] = s.id
    return pars, s

  def create_device(self):
    conf = {'label' : 'foo-%f' % time.time(),
            'maker' : 'foomaker',
            'model' : 'foomodel',
            'release' : '%f' % time.time()}
    device = self.kb.factory.create(self.kb.Device, conf)
    conf['id'] = device.id
    return conf, device

  def create_hardware_device(self):
    conf = {'label' : 'foo-%f' % time.time(),
            'maker' : 'foomaker',
            'model' : 'foomodel',
            'release' : '%f' % time.time(),
            'barcode' : '%f' % time.time(),
            'physicalLocation' : 'HERE_THERE_EVERYWHERE'}
    device = self.kb.factory.create(self.kb.HardwareDevice, conf)
    conf['id'] = device.id
    return conf, device

  def create_action_setup(self, action_setup=None):
    conf = {'label' : 'asetup-%f' % time.time(),
            'conf' : '{"param1": "foo"}'}
    if action_setup is None:
      action_setup = self.kb.factory.create(self.kb.ActionSetup, conf)
    conf['id'] = action_setup.id
    return conf, action_setup

  def create_action(self, action_klass=None, target=None):
    dev_conf, device = self.create_device()
    self.kill_list.append(device.save())
    asu_conf, asetup = self.create_action_setup()
    self.kill_list.append(asetup.save())
    stu_conf, study = self.create_study()
    self.kill_list.append(study.save())
    conf = {'setup' : asetup,
            'device': device,
            'actionCategory' : self.kb.ActionCategory.IMPORT,
            'operator' : 'Alfred E. Neumann',
            'context'  : study,
            'description' : 'description ...',
            'target' : target
            }
    action_klass = action_klass if action_klass else self.kb.Action
    action = self.kb.factory.create(action_klass, conf)
    return conf, action

  def create_action_on_individual(self, individual=None):
    if not individual:
      conf, individual = self.create_individual()
      self.kill_list.append(individual.save())
    return self.create_action(action_klass=self.kb.ActionOnIndividual,
                              target=individual)

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
    return self.create_action(action_klass=self.kb.ActionOnDataSample,
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

  def create_vessel_conf_helper(self, action=None):
    if not action:
      aconf, action = self.create_action()
      self.kill_list.append(action.save())
    conf = {
      'currentVolume' : 0.2,
      'initialVolume' : 0.2,
      'content'       : self.kb.VesselContent.BLOOD,
      'status'        : self.kb.VesselStatus.CONTENTUSABLE,
      'action'        : action
      }
    return conf

  def create_vessel(self, action=None):
    conf = self.create_vessel_conf_helper(action)
    v = self.kb.factory.create(self.kb.Vessel, conf)
    return conf, v

  def create_tube(self, action=None):
    conf = self.create_vessel_conf_helper(action)
    conf['label'] = 'tl-%s'  % time.time()
    v = self.kb.factory.create(self.kb.Tube, conf)
    return conf, v

  def create_data_sample_conf_helper(self, action=None):
    if not action:
      aconf, action = self.create_action()
      self.kill_list.append(action.save())
    conf = {
      'label'  : 'ds-label-%s' % time.time(),
      'status' : self.kb.DataSampleStatus.USABLE,
      'action' : action
      }
    return conf

  def create_data_sample(self, action=None):
    conf = self.create_data_sample_conf_helper(action)
    ds = self.kb.factory.create(self.kb.DataSample, conf)
    return conf, ds

  def create_snp_markers_set(self):
    conf = {'label' : 'label-%s' % time.time(),
            'maker' : 'maker-%s' % time.time(),
            'model' : 'model-%s' % time.time(),
            'release' : 'release-%s' % time.time(),
            'markersSetVID' : 'V-%s' % time.time()}
    sms = self.kb.factory.create(self.kb.SNPMarkersSet, conf)
    return conf, sms

  def create_genotype_data_sample(self, action=None):
    conf = self.create_data_sample_conf_helper(action)
    conf['label'] = 'label-%s' % time.time()
    sconf, sms = self.create_snp_markers_set()
    self.kill_list.append(sms.save())
    conf['snpMarkersSet'] = sms
    gds = self.kb.factory.create(self.kb.GenotypeDataSample, conf)
    return conf, gds

  def create_data_object(self, data_sample=None):
    if not data_sample:
      dconf, data_sample = self.create_data_sample()
      self.kill_list.append(data_sample.save())
    conf = {'sample' : data_sample,
            'path'   : 'hdfs://a.path',
            'mimetype' : 'x-affy/cel',
            'sha1'     : '3u2398989',
            'size'     : 19209092L}
    do = self.kb.factory.create(self.kb.DataObject, conf)
    return conf, do

  def create_collection_conf_helper(self, action=None):
    if not action:
      aconf, action = self.create_action()
      self.kill_list.append(action.save())
    conf = {
      'label'  : 'col-%s' % time.time(),
      'action' : action
      }
    return conf

  def create_container(self, action=None):
    conf = self.create_collection_conf_helper(action)
    conf['barcode'] =  '9898989-%s' % time.time()
    conf['status']  = self.kb.ContainerStatus.READY
    c = self.kb.factory.create(self.kb.Container, conf)
    return conf, c

  def create_slotted_container(self, action=None):
    conf = self.create_collection_conf_helper(action)
    conf['numberOfSlots'] =  16
    conf['barcode'] =  '9898989-%s' % time.time()
    conf['status']  = self.kb.ContainerStatus.READY
    c = self.kb.factory.create(self.kb.SlottedContainer, conf)
    return conf, c

  def create_titer_plate(self, action=None):
    conf = self.create_collection_conf_helper(action)
    conf['rows'] =  8
    conf['columns'] =  12
    conf['barcode'] =  '9898989-%s' % time.time()
    conf['status']  = self.kb.ContainerStatus.READY
    c = self.kb.factory.create(self.kb.TiterPlate, conf)
    return conf, c

  def create_data_collection(self, action=None):
    conf = self.create_collection_conf_helper(action)
    c = self.kb.factory.create(self.kb.DataCollection, conf)
    return conf, c

  def create_data_collection_item(self, data_collection=None,
                                  data_sample=None):
    if not data_collection:
      dconf, data_collection = self.create_data_collection()
      self.kill_list.append(data_collection.save())
    if not data_sample:
      dconf, data_sample = self.create_data_sample()
      self.kill_list.append(data_sample.save())
    conf = {'dataSample' : data_sample,
            'dataCollection' : data_collection}
    dci = self.kb.factory.create(self.kb.DataCollectionItem, conf)
    return conf, dci

  def create_plate_well(self, titer_plate, slot=None, label=None, action=None):
    conf = self.create_vessel_conf_helper(action)
    if not label is None:
      conf['label'] = label
    if not slot is None:
      conf['slot'] = slot
    if not (label or slot):
      conf['label'] = 'A01'
    conf['container'] = titer_plate
    w = self.kb.factory.create(self.kb.PlateWell, conf)
    return conf, w

  def create_individual(self, action=None, gender=None, father=None,
                        mother=None):
    if not action:
      aconf, action = self.create_action()
      self.kill_list.append(action.save())
    conf = {'gender' : gender if gender else self.kb.Gender.MALE,
            'action' : action}
    if father:
      conf['father'] = father
    if mother:
      conf['mother'] = mother

    ind = self.kb.factory.create(self.kb.Individual, conf)
    return conf, ind

  def create_enrollment(self, study=None, individual=None, st_code=None):
    if not study:
      sconf, study = self.create_study()
      self.kill_list.append(study.save())
    if not individual:
      iconf, individual = self.create_individual()
      self.kill_list.append(individual.save())
    if not st_code:
      st_code = 'st-code-%s' % time.time()
    conf = {'study' : study,
            'individual' : individual,
            'studyCode'  : st_code}
    e = self.kb.factory.create(self.kb.Enrollment, conf)
    return conf, e
