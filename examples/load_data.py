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

@debug_wrapper
def omero_id(obj):
  if hasattr(obj, 'omero_id'):
    ome_id = obj.omero_id
  else:
    ome_id = obj.id._val
  logger.debug('\tobject id is %s' % ome_id)
  return ome_id


@debug_wrapper
def same_object(obj1, obj2):
  return omero_id(obj1) == omero_id(obj2)


class network_builder(object):

  def __init__(self, host, user, passwd, study_label):
    self.skb = sKB(driver='omero')(host, user, passwd)
    self.ikb = iKB(driver='omero')(host, user, passwd)
    self.gkb = gKB(driver='omero')(host, user, passwd)
    self.acat_map   = self.skb.get_action_category_table()
    self.outcome_map = self.skb.get_result_outcome_table()
    self.sstatus_map = self.skb.get_sample_status_table()
    self.dtype_map   = self.skb.get_data_type_table()
    self.gender_map = self.ikb.get_gender_table()

    self.study_label = 'FOO-%f' % time.time()

    s = self.skb.Study(label=self.study_label)
    self.study = self.skb.save(s)
    self.enrollments = []
    self.individuals = []

    #
    self.logger = logger

  #--------------------------------------------------------------------------------------------------------
  #
  # ACTIONS DEFINITION
  #
  #--------------------------------------------------------------------------------------------------------

  @debug_wrapper
  def create_action_helper(self, aclass, study, device, asetup, acat, operator, target=None):
    desc = "This is a simulation"
    action = aclass()
    action.setup, action.device, action.actionCategory = asetup, device, acat
    action.operator, action.context, action.description = operator, study, desc
    if target:
      action.target = target
    try:
      return self.skb.save(action)
    except KBError, e:
      print 'got an error:', e
      print 'action:', action
      print 'action.ome_obj:', action.ome_obj
      raise KBerror()

  def create_action(self, device, asetup, acat, operator):
    return self.create_action_helper(self.skb.Action,
                                     self.study, device, asetup, acat, operator)

  def create_action_on_individual(self, enrollment, device, asetup, acat, operator):
    return self.create_action_helper(self.ikb.ActionOnIndividual,
                                     enrollment.study, device, asetup, acat, operator,
                                     enrollment.individual)

  @debug_wrapper
  def create_action_on_sample(self, sample, device, asetup, acat, operator):
    return self.create_action_helper(self.skb.ActionOnSample,
                                     self.study, device, asetup, acat, operator,
                                     sample)

  def create_action_on_sample_slot(self, sample_slot, device, asetup, acat, operator):
    return self.create_action_helper(self.skb.ActionOnSamplesContainerSlot,
                                     self.study, device, asetup, acat, operator,
                                     sample_slot)

  def create_action_on_data_collection(self, data_collection, device, asetup, acat, operator):
    return self.create_action_helper(self.skb.ActionOnDataCollection,
                                     self.study, device, asetup, acat, operator,
                                     data_collection)

  def create_action_on_data_collection_item(self, item, device, asetup, acat, operator):
    return self.create_action_helper(self.skb.ActionOnDataCollectionItem,
                                     self.study, device, asetup, acat, operator,
                                     item)

  #------------------------------------------------------------------------
  #
  # OBJECTS DEFINITION
  #
  #------------------------------------------------------------------------
  @debug_wrapper
  def get_device(self, maker, model, release):
    label = '%s-%s-%s' % (maker, model, release)
    device = self.skb.get_device(label)
    if not device:
      self.logger.debug('creating device %s [%s,%s,%s]' % (label,
                                                           maker, model,
                                                           release))
      device = self.skb.Device(label=label,
                               maker=maker, model=model, release=release)
      device = self.skb.save(device)
    return device

  @debug_wrapper
  def get_markers_set(self, maker, model, release, set_vid=None):
    snp_markers_set = self.skb.get_snp_markers_set(maker, model, release)
    if not snp_markers_set:
      assert set_vid
      self.logger.debug('creating a SMPMarkersSet instance [%s,%s,%s,%s]' % (maker, model,
                                                                             release, set_vid))
      device = self.get_device('CRS4', 'FAKE-snp_markers_set-builder', '0.0')
      asetup = self.get_action_setup('import-prog-%f' % time.time(),
                                     '{"foo2": "foo"}')
      acat  = self.acat_map['IMPORT']
      operator = 'Alfred E. Neumann'
      action = self.create_action(device, asetup, acat, operator)
      #--
      snp_markers_set = self.skb.SNPMarkersSet(maker=maker, model=model, release=release,
                                               set_vid=set_vid)
      snp_markers_set.action = action
      snp_markers_set = self.skb.save(snp_markers_set)
    return snp_markers_set

  @debug_wrapper
  def get_action_setup(self, label, conf):
    asetup = self.skb.ActionSetup(label=label)
    asetup.conf = conf
    return self.skb.save(asetup)

  @debug_wrapper
  def create_individual(self, gender):
    device = self.get_device('CRS4', 'IMPORT', '0.0')
    asetup = self.get_action_setup('import-prog-%f' % time.time(),
                                   '{"foo2": "foo"}')
    acat  = self.acat_map['IMPORT']
    operator = 'Alfred E. Neumann'
    action = self.create_action(device, asetup, acat, operator)
    i = self.ikb.Individual(gender=gender)
    i.action = action
    return i

  @debug_wrapper
  def create_blood_sample(self, enrollment, device, asetup,
                          acat, operator, volume):
    action = self.create_action_on_individual(enrollment, device, asetup, acat, operator)
    #--
    sample = self.skb.BloodSample()
    sample.action, sample.outcome   = action, self.outcome_map['OK']
    sample.label = '%s-%s' % (self.study_label, enrollment.studyCode)
    sample.barcode  = sample.id
    sample.initialVolume = sample.currentVolume = volume
    sample.status = self.sstatus_map['USABLE']
    return sample

  @debug_wrapper
  def create_dna_sample(self, blood_sample, device, asetup, acat, operator):
    action = self.create_action_on_sample(blood_sample, device,
                                          asetup, acat, operator)
    #-
    sample = self.skb.DNASample()
    sample.action, sample.outcome   = action, self.outcome_map['OK']
    sample.label = '%s-DNA-%s' % (blood_sample.label, time.time())
    sample.barcode  = sample.id
    sample.initialVolume = sample.currentVolume = 0.1
    sample.nanodropConcentration = 40
    sample.qp230260 = sample.qp230280 = 0.3
    sample.status = self.sstatus_map['USABLE']
    return sample

  @debug_wrapper
  def create_raw_genotype_measure(self, sample_slot,
                                  device, asetup, acat, operator):
    action = self.create_action_on_sample_slot(sample_slot, device,
                                               asetup, acat, operator)

    # FIXME: assigning data_type here is stupid: it can only be a
    # 'GTRAW'. We need to do this because the constructor does not
    # have access to the dtype_map. There should be a FactoryClass in
    # the kb that does this under the hood..
    data_sample = self.skb.AffymetrixCel(name='foo-%f.cel' % time.time(),
                                         array_type='GenomeWideSNP_6',
                                         data_type=self.dtype_map['GTRAW'])
    data_sample.action  = action
    data_sample.outcome = self.outcome_map['OK']
    data_sample = self.skb.save(data_sample)
    #-
    #FIXME: We are assuming that the measuring process generated a physical file
    #
    path = 'file://ELS/els5/storage/a/%s' % data_sample.name
    sha1 = 'a fake sha1 of %s' % data_sample.name
    mime_type = 'x-application/affymetrix-cel' # FIXME, we need to list the legal mime_types
    size = 0 # the actual file size
    data_object = self.skb.DataObject(sample=data_sample,
                                      mime_type=mime_type,
                                      path=path, sha1=sha1, size=size)
    data_object = self.skb.save(data_object)
    return data_sample, data_object

  @debug_wrapper
  def create_data_collection_item(self, device, asetup, acat, operator, data_collection, data_sample):
    self.logger.debug('data_collection: %s[%s] data_sample: %s[%s]' % (data_collection.get_ome_table(),
                                                                       data_collection.id,
                                                                       data_sample.get_ome_table(),
                                                                       data_sample.id))
    action = self.create_action_on_sample(data_sample, device,
                                          asetup, acat, operator)
    action = self.skb.save(action)
    #-
    data_collection_item = self.skb.DataCollectionItem(data_collection=data_collection,
                                                       data_sample=data_sample)
    data_collection_item.action = action
    data_collection_item = self.skb.save(data_collection_item)

  @debug_wrapper
  def create_called_genotype_data(self, item, device, markers_set, asetup, acat,  operator):
    action = self.create_action_on_data_collection_item(item, device, asetup, acat, operator)
    #-
    data_sample = self.skb.GenotypeDataSample(name='foo-%f.gc' % time.time(),
                                              snp_markers_set=markers_set,
                                              data_type=self.dtype_map['GTCALL'])
    data_sample.action  = action
    data_sample.outcome = self.outcome_map['OK']
    data_sample = self.skb.save(data_sample)
    #--
    # FIXME
    # (vid, path, sha1, mime_type) = self.gkb.append_gdo(set_vid, probs, confidence,
    #                                                    data_sample.action.id)
    path = 'table:table<xxx>.h5/<%s>' % data_sample.name
    sha1 = 'a fake sha1 of %s' % data_sample.name
    mime_type = 'x-application/gdo' # FIXME, we need to list the legal mime_types
    size = 0 # the actual dataobject size
    data_object = self.skb.DataObject(sample=data_sample, mime_type=mime_type,
                                      path=path, sha1=sha1, size=size)
    data_object = self.skb.save(data_object)
    return data_sample, data_object

  #--------------------------------------------------------------------------------------------------------
  #
  # INDIVIDUALS REGISTRATION AND ENROLLMENT
  #
  #--------------------------------------------------------------------------------------------------------

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
      i = self.create_individual(gender=self.gender_map[x.gender])
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

  #--------------------------------------------------------------------------------------------------------
  #
  # BIO SAMPLES RELATED OPERATIONS
  #
  #--------------------------------------------------------------------------------------------------------

  @debug_wrapper
  def acquire_blood_samples(self, conf):
    device = self.get_device(conf['organization'], conf['department'], '0.0')
    asetup = self.get_action_setup('blood-samples-conf-%f' % time.time(),
                                   conf['action-conf'])
    acat  = self.acat_map['ACQUISITION']

    for e in self.enrollments:
      sample = self.create_blood_sample(e, device, asetup,
                                        acat, conf['operator'], conf['volume'])
      self.skb.save(sample)


  @debug_wrapper
  def extract_dna_samples(self):
    device = self.get_device('ACME corp', 'DNA magic extractor', '0.0')
    asetup = self.get_action_setup('dna-sample-conf-%f' % time.time(),
                                   '{"foo" : "a-value"}')
    acat  = self.acat_map['EXTRACTION']

    for e in self.enrollments:
      blood_sample = self.skb.get_descendants(e.individual, self.skb.BloodSample)[0]
      assert blood_sample
      sample = self.create_dna_sample(blood_sample, device,
                                      asetup, acat, 'Wiley E. Coyote')
      self.skb.save(sample)

  #--------------------------------------------------------------------------------------------------------
  #
  # FILLING TITER PLATES
  #
  #--------------------------------------------------------------------------------------------------------

  @debug_wrapper
  def fill_titer_plate(self, device, asetup, acat,
                       operator,
                       rows, columns, barcode, stream):
    plate = self.skb.TiterPlate(rows=rows, columns=columns, barcode=barcode)
    plate.action = self.create_action(device, asetup, acat, operator)
    plate = self.skb.save(plate)
    delta_volume = 0.01

    for r in range(plate.rows):
      for c in range(plate.columns):
        try:
          dna_sample = stream.next()
        except StopIteration:
          return False, plate
        action = self.create_action_on_sample(dna_sample, device,
                                              asetup, acat, operator)
        label = '%s.%02d%02d' % (plate.label, r, c)
        plate_well = self.skb.PlateWell(label=label,
                                        sample=dna_sample, container=plate,
                                        row=r, column=c,
                                        volume=delta_volume)
        plate_well.action = action
        plate_well.outcome = self.outcome_map['OK']
        plate_well = self.skb.save(plate_well)
        current_volume = dna_sample.currentVolume - delta_volume
        assert current_volume >= 0
        dna_sample.currentVolume = current_volume
        dna_sample = self.skb.save(dna_sample)
    return True, plate

  @debug_wrapper
  def fill_titer_plates(self):
    rows, columns = 3, 3
    device = self.get_device('ACME corp', 'DNA magic aliquot dispenser', '0.0')
    asetup = self.get_action_setup('dna-aliquot-conf-%f' % time.time(),
                                   '{"foo" : "a-value"}')
    acat  = self.acat_map['EXTRACTION'] # ???
    operator = "Alfred E. Neuman"

    def dna_sample_stream():
      for e in self.enrollments:
        dna_sample = self.skb.get_descendants(e.individual, self.skb.DNASample)[0]
        yield dna_sample

    stream = dna_sample_stream()
    barcode = 'titer-plate-bc-%f' % time.time()
    plates_to_fill, plate = self.fill_titer_plate(device, asetup, acat, operator,
                                                  rows, columns, barcode, stream)
    while plates_to_fill:
      barcode = 'titer-plate-bc-%f' % time.time()
      plates_to_fill, plate = self.fill_titer_plate(device, asetup, acat, operator,
                                                    rows, columns, barcode, stream)

  #--------------------------------------------------------------------------------------------------------
  #
  # GENOTYPES, RAW AND CALLED
  #
  #--------------------------------------------------------------------------------------------------------

  @debug_wrapper
  def measure_raw_genotypes(self, maker, model):
    #FIXME: asetup should be linked to the specific action device
    device = self.get_device(maker, model, '0.0')
    asetup = self.get_action_setup('affy6-%f' % time.time(),
                                   '{foo2: "foo"}')
    acat  = self.acat_map['PROCESSING']

    for p in self.skb.get_titer_plates():
      for w in self.skb.get_wells_of_plate(p):
        self.create_raw_genotype_measure(w, device, asetup,
                                         acat, 'Wiley E. Coyote')

  @debug_wrapper
  def build_data_collection(self):
    device = self.get_device('CRS4', 'data-collection-producer', '0.0')
    asetup = self.get_action_setup('data-collection-producer-conf-%f' % time.time(),
                                   '{"foo2": "foo"}')
    acat  = self.acat_map['PROCESSING']
    operator = 'Alfred E. Neumann'
    #-
    data_collection = self.skb.DataCollection(study=self.study)
    data_collection = self.skb.save(data_collection)
    #-
    for e in self.enrollments:
      data_samples = self.skb.get_descendants(e.individual, self.skb.AffymetrixCel)
      data_sample = [ds for ds in data_samples
                     if same_object(ds.dataType, self.dtype_map['GTRAW'])][0]
      #data_collection.append_item(dataSample=data_sample)
      self.create_data_collection_item(device, asetup, acat, operator, data_collection, data_sample)
    #--
    return data_collection

  @debug_wrapper
  def call_genotypes(self):
    data_collection = self.build_data_collection()
    #-
    device = self.get_device('CRS4', 'MR-birdseed', '0.0')
    asetup = self.get_action_setup('mr-birdseed-conf-%f' % time.time(),
                                   '{"foo2": "foo"}')
    acat  = self.acat_map['PROCESSING']
    #--- FIXME this should be moved to an internal detail of skb and gkb
    maker, model = 'Affymetrix', 'GenomeWideSNP_6'
    selector = "(maker == '%s')&(model== '%s')"  % (maker, model)
    #-- FIXME this is disabled, since we have not defined yet this snp_markers_set
    #set_vid = self.gkb.get_snp_markers_sets(selector=selector)[0]
    set_vid = 'A-FAKE-VID'
    #-
    markers_set = self.get_markers_set(maker, model, release='1.0', set_vid=set_vid)
    #for item in data_collection.items():
    for item in self.skb.get_data_collection_items(data_collection):
      self.create_called_genotype_data(item, device, markers_set, asetup, acat,  'Wiley E. Coyote')

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
  nb.measure_raw_genotypes(maker='affymetrix', model='GenomeWide6.0')
  logger.info('raw genotypes acquired')
  nb.call_genotypes()
  logger.info('genotypes called')

main()





