"""

Load Data
=========


This example shows how one can import a full connected network of
individuals and objects. Specifically, we will:

  * create a study;

  * load a group of individuals and enroll them in the study;

  * for each individual define blood samples, derived samples, and
    derived experimental results.


"""

from bl.vl.sample.kb     import KBError
from bl.vl.sample.kb     import KnowledgeBase as sKB
from bl.vl.individual.kb import KnowledgeBase as iKB
from bl.vl.genotype.kb   import KnowledgeBase as gKB
import numpy as np
import time
import os, sys
import logging

LOG_FILENAME = 'load_data.log'
logging.basicConfig(filename=LOG_FILENAME,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

logger = logging.getLogger("example_data_loader")

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter()

ch.setFormatter(formatter)

logger.addHandler(ch)

INDIVIDUALS = [(0, None, None, 'MALE'), (1, None, None, 'FEMALE'),
               (2, 0, 1, 'MALE'), (3, 0, 1, 'FEMALE')]

counter = 0
def debug_wrapper(f):
  def debug_wrapper_wrapper(*args, **kv):
    global counter
    now = time.time()
    counter += 1
    logger.debug('%s[%d] in' % (f.__name__, counter))
    res = f(*args, **kv)
    logger.debug('%s[%d] out (%f)' % (f.__name__, counter, time.time() - now))
    counter -= 1
    return res
  return debug_wrapper_wrapper

class network_builder(object):

  def __init__(self, host, user, passwd, study_label):
    self.skb = sKB(driver='omero')(host, user, passwd)
    self.ikb = iKB(driver='omero')(host, user, passwd)
    self.gkb = gKB(driver='omero')(host, user, passwd)
    self.atype_map   = self.skb.get_action_type_table()
    self.outcome_map = self.skb.get_result_outcome_table()
    self.sstatus_map = self.skb.get_sample_status_table()
    self.dtype_map   = self.skb.get_data_type_table()
    self.gender_map = self.ikb.get_gender_table()

    self.study_label = 'FOO-EXPERIMENT-%f' % time.time()

    s = self.skb.Study(label=self.study_label)
    self.study = self.skb.save(s)
    self.enrollments = []
    self.individuals = []

    #
    self.logger = logger

  @debug_wrapper
  def register_in_db(self, i_map):
    work_to_do = False
    for k in i_map.keys():
      registered, i, father, mother = i_map[k]
      if registered:
        continue
      if father is None and mother is None:
        i_map[k] = (True, self.ikb.save(i), None, None)
      else:
        work_to_do = True
    return work_to_do

  @debug_wrapper
  def update_parents(self, i_map):
    for k in i_map.keys():
      registered, i, father, mother = i_map[k]
      if registered:
        continue
      if not father is None and i_map[father][0]:
        self.logger.debug('registered father for %s' % i.id)
        i.father = i_map[father][1]
        father = None
      if not mother is None and i_map[mother][0]:
        self.logger.debug('registered mother for %s' % i.id)
        i.mother = i_map[mother][1]
      i_map[k] = (False, i, None, None)

  @debug_wrapper
  def register_individuals(self, i_stream):
    i_map = {}
    for x in i_stream:
      i = self.ikb.Individual(gender=self.gender_map[x.gender])
      i_map[x.label] = (False, i, x.father, x.mother)

    work_to_do = self.register_in_db(i_map)
    while work_to_do:
      self.update_parents(i_map)
      work_to_do = self.register_in_db(i_map)
    self.individuals = [x[1] for x in i_map.values()]

  @debug_wrapper
  def enroll_individuals(self, individuals):
    self.register_individuals(individuals)
    for k, i in enumerate(self.individuals):
      e = self.ikb.Enrollment(study=self.study, individual=i,
                              study_code= '%d' % k)
      self.enrollments.append(self.ikb.save(e))

  def get_device(self, vendor, model, release):
    device = self.skb.get_device(vendor, model, release)
    if not device:
      self.logger.debug('creating a new device')
      device = self.skb.Device(vendor=vendor, model=model, release=release)
      device = self.skb.save(device)
    return device

  def get_action_setup(self, label, conf):
    asetup = self.skb.ActionSetup(label=label)
    asetup.conf = conf
    return self.skb.save(asetup)

  @debug_wrapper
  def get_action_helper(self, aclass, study, target, device, asetup, atype, operator):
    desc = "This is a simulation"
    action = aclass()
    action.setup, action.device, action.actionType = asetup, device, atype
    action.operator, action.context, action.description = operator, study, desc
    action.target = target
    try:
      return self.skb.save(action)
    except KBError, e:
      print 'got an error:', e
      print 'action:', action
      print 'action.ome_obj:', action.ome_obj
      sys.exit(1)


  def get_action_on_individual(self, enrollment, device, asetup, atype, operator):
    return self.get_action_helper(self.ikb.ActionOnIndividual,
                                  enrollment.study, enrollment.individual,
                                  device, asetup, atype, operator)

  @debug_wrapper
  def get_action_on_sample(self, sample, device, asetup, atype, operator):
    return self.get_action_helper(self.skb.ActionOnSample,
                                  self.study, sample,
                                  device, asetup, atype, operator)

  def get_action_on_sample_slot(self, sample_slot, device, asetup, atype, operator):
    return self.get_action_helper(self.skb.ActionOnSampleSlot,
                                  self.study, sample_slot,
                                  device, asetup, atype, operator)

  def get_action_on_data_collection(self, data_collection, device, asetup, atype, operator):
    return self.get_action_helper(self.skb.ActionOnDataCollection,
                                  self.study, data_collection,
                                  device, asetup, atype, operator)

  def get_action_on_data_collection_item(self, item, device, asetup, atype, operator):
    return self.get_action_helper(self.skb.ActionOnDataCollectionItem,
                                  self.study, item,
                                  device, asetup, atype, operator)

  @debug_wrapper
  def acquire_blood_sample(self, enrollment, device, asetup,
                           atype, operator, volume):
    action = self.get_action_on_individual(enrollment, device, asetup, atype, operator)
    #--
    sample = self.skb.BloodSample()
    sample.action, sample.outcome   = action, self.outcome_map['PASSED']
    sample.labLabel = '%s-%s' % (enrollment.study.label, enrollment.studyCode)
    sample.barcode  = sample.id
    sample.initialVolume = sample.currentVolume = volume
    sample.status = self.sstatus_map['USABLE']
    return self.skb.save(sample)

  @debug_wrapper
  def acquire_blood_samples(self, conf):
    device = self.get_device(conf['organization'], conf['department'], '0.0')
    asetup = self.get_action_setup('blood-samples-conf-%f' % time.time(),
                                   conf['action-conf'])
    atype  = self.atype_map['ACQUISITION']

    for e in self.enrollments:
      self.acquire_blood_sample(e, device, asetup, atype, conf['operator'], conf['volume'])


  @debug_wrapper
  def extract_dna_sample(self, blood_sample, device, asetup, atype, operator):
    action = self.get_action_on_sample(blood_sample, device,
                                       asetup, atype, operator)
    action = self.skb.save(action)
    #-
    sample = self.skb.DNASample()
    sample.action, sample.outcome   = action, self.outcome_map['PASSED']
    sample.labLabel = '%s-DNA-%s' % (blood_sample.labLabel, time.time())
    sample.barcode  = sample.id
    sample.initialVolume = sample.currentVolume = 0.1
    sample.nanodropConcentration = 40
    sample.qp230260 = sample.qp230280 = 0.3
    sample.status = self.sstatus_map['USABLE']
    return self.skb.save(sample)

  @debug_wrapper
  def extract_dna_samples(self):
    device = self.get_device('ACME corp', 'DNA magic extractor', '0.0')
    asetup = self.get_action_setup('dna-sample-conf-%f' % time.time(),
                                   '{"foo" : "a-value"}')
    atype  = self.atype_map['EXTRACTION']

    for e in self.enrollments:
      blood_sample = self.skb.get_descendants(e.individual, self.skb.BloodSample)[0]
      assert blood_sample
      self.extract_dna_sample(blood_sample, device,
                              asetup, atype, 'Wiley E. Coyote')

  @debug_wrapper
  def fill_titer_plate(self, device, asetup, atype,
                       operator,
                       rows, columns, barcode, stream):
    plate = self.skb.TiterPlate(rows=rows, columns=columns, barcode=barcode)
    plate.barcode
    plate = self.skb.save(plate)
    delta_volume = 0.01

    for r in range(plate.rows):
      for c in range(plate.columns):
        try:
          dna_sample = stream.next()
        except StopIteration:
          return False, plate
        action = self.get_action_on_sample(dna_sample, device,
                                           asetup, atype, operator)
        plate_well = self.skb.PlateWell(sample=dna_sample, container=plate,
                                        row=r, column=c,
                                        volume=delta_volume)
        print 'action:', action
        plate_well.action = action
        plate_well.outcome = self.outcome_map['PASSED']
        print 'plate_well:', plate_well.ome_obj
        plate_well = self.skb.save(plate_well)
        dna_sample.volume = dna_sample.volume - delta_volume
        dna_sample = self.skb.save(dna_sample)
    return True, plate

  @debug_wrapper
  def fill_titer_plates(self):
    rows, columns = 3, 3
    device = self.get_device('ACME corp', 'DNA magic aliquot dispenser', '0.0')
    asetup = self.get_action_setup('dna-aliquot-conf-%f' % time.time(),
                                   '{"foo" : "a-value"}')
    atype  = self.atype_map['EXTRACTION'] # ???
    operator = "Alfred E. Neuman"

    def dna_sample_stream():
      for e in self.enrollments:
        yield self.ikb.get_dna_sample(individual=e.individual)

    stream = dna_sample_stream()
    barcode = 'titer-plate-bc-%f' % time.time()
    plates_to_fill, plate = self.fill_titer_plate(device, asetup, atype, operator,
                                                  rows, columns, barcode, stream)
    while plates_to_fill:
      barcode = 'titer-plate-bc-%f' % time.time()
      plates_to_fill, plate = self.fill_titer_plate(device, asetup, atype, operator,
                                                    rows, columns, barcode, stream)

  @debug_wrapper
  def measure_raw_genotype(self, sample, device, asetup, atype, operator):
    action = self.get_action_on_sample_slot(sample, device, asetup, atype, operator)
    #-
    sample = self.skb.DataSample(name='FIXME-this-is-a-workaround')
    sample.action  = action
    sample.outcome = self.outcome_map['PASSED']
    sample.name = '%s.cel' % sample.id
    sample.dataType = self.dtype_map['GTRAW']
    sample = self.skb.save(sample)
    #-
    #FIXME: We are assuming that the measuring process generated a physical file
    #
    path = 'file://ELS/els5/storage/a/%s' % data_sample.name
    sha1 = 'a fake sha1 of %s' % data_sample.name
    mime_type = 'x-application/affymetrix-cel' # FIXME, we need to list the legal mime_types
    size = 0 # the actual file size
    data_object = self.skb.DataObject(sample=sample, mime_type=mime_type,
                                      path=path, sha1=sha1, size=size)
    data_object = self.skb.save(data_object)
    return sample, data_object

  @debug_wrapper
  def measure_raw_genotypes(self):
    #FIXME: asetup should be linked to the specific action device
    device = self.get_device('Affymetrix', 'GenomeWide 6.0', '0.0')
    asetup = self.get_action_setup('affy6-%f' % time.time(),
                                   '{foo2: "foo"}')
    asetup = self.get_action_setup('nothing to declare')
    atype  = self.atype_map['PROCESSING']

    for p in self.skb.get_titer_plates():
      for w in self.skb.get_wells_of_plate(p):
        self.measure_raw_genotype(w.sample, device, asetup,
                                  atype, 'Wiley E. Coyote')

  @debug_wrapper
  def build_data_collection(self):
    data_collection = self.skb.DataCollection(study=self.study)
    data_collection = self.skb.save(data_collection)
    #-
    for e in self.enrollments:
      data_sample = self.ikb.get_dataset(individual=e.individual, dataType=self.dtype_map['GTRAW'])
      data_collection.append_item(dataSample=data_sample)
    #--
    return data_collection

  @debug_wrapper
  def call_genotype(self, item, device, asetup, atype,  operator):
    action = self.get_action_on_data_collection_item(item, device, asetup, atype, operator)
    #-
    sample = self.skb.DataSample(name='FIXME-this-is-a-workaround')
    sample.action  = action
    sample.outcome = self.outcome_map['PASSED']
    sample.name = '%s.gt' % sample.id
    sample.dataType = self.dtype_map['GTCALL']
    sample = self.skb.save(sample)
    #--
    # FIXME
    # (vid, path, sha1, mime_type) = self.gkb.append_gdo(set_vid, probs, confidence,
    #                                                    data_sample.action.id)
    path = 'table:table<xxx>.h5/<%s>' % sample.name
    sha1 = 'a fake sha1 of %s' % sample.name
    mime_type = 'x-application/gdo' # FIXME, we need to list the legal mime_types
    size = 0 # the actual dataobject size
    data_object = self.skb.DataObject(name=sample.name, mime_type=mime_type,
                                      path=path, sha1=sha1, size=size)
    data_object = self.skb.save(data_object)
    return sample, data_object

  @debug_wrapper
  def call_genotypes(self):
    device = self.get_device('CRS4', 'MR-birdseed', '0.0')
    asetup = self.get_action_setup('mr-birdseed-conf-%f' % time.time(),
                                   '{"foo2": "foo"}')
    atype  = self.atype_map['PROCESSING']
    selector = "(vendor == ''Affymetrix')&(model== 'GenomeWide 6.0')"
    set_vid = self.gkb.get_snp_markers_set(selector=selector)

    data_collection = self.build_data_collection()
    #-
    for item in data_collection.items():
      self.call_genotype(item, device, asetup, atype,  'Wiley E. Coyote')

def main():
  OME_HOST = os.getenv("OME_HOST", "localhost")
  OME_USER = os.getenv("OME_USER", "root")
  OME_PASS = os.getenv("OME_PASS", "romeo")

  nb = network_builder(OME_HOST, OME_USER, OME_PASS, study_label='foo')
  logger.info('ready to enroll')

  def stream(ind_array):
    class I(object):
      def __init__(self, ilabel, flabel, mlabel, gender):
        self.label = ilabel
        self.father, self.mother, self.gender = flabel, mlabel, gender
    for x in ind_array:
      yield I(*x)

  nb.enroll_individuals(stream(INDIVIDUALS))
  logger.info('enrolled')

  nb.acquire_blood_samples({'organization' : 'Azienda Ospedaliera Brotzu',
                            'department'   : 'Centro trasfusionale',
                            'action-conf' : '{"protocol" : "a-protocol"}',
                            'operator' : 'Alfred E. Neuman',
                            'volume' : 100.0})
  logger.info('acquired blood samples')
  nb.extract_dna_samples()
  logger.info('dna samples extracted')
  nb.fill_titer_plates()
  logger.info('titer plate filled')
  nb.measure_raw_genotypes(vendor='affymetrix', model='GenomeWide6.0')
  logger.info('raw genotypes acquired')
  nb.call_genotypes()
  logger.info('genotypes called')

main()





