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

from bl.lib.sample.kb     import KnowledgeBase as sKB
from bl.lib.individual.kb import KnowledgeBase as iKB
from bl.lib.genotype.kb   import KnowledgeBase as gKB
import numpy as np
import time

# o = omero.model.OriginalFileI()

# o.mimetype = 'x-omero-bl/affymetrix-cel'
# o.path = 'hdfs://host:93090/data/foo.cel'

# o.name = <object-vid>
# o.mimetype = 'x-omero-bl/<marker-set-vid>'
# o.path = 'hf:<gdo-table-name>:<rowid>'

INDIVIDUALS = [(0, None, None, 'MALE'), (1, None, None, 'FEMALE'),
               (2, 0, 1, 'MALE'), (3, 0, 1, 'FEMALE')]

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

    s = self.skb.Study(label=study_label)
    self.study = self.skb.save(s)
    self.enrollments = []
    self.individuals = []

  def register_individuals(self, individuals):
    """
    Create and register a group of individuals.
    """
    gender_table = ikb.get_gender_table()
    i_map = {}
    founders = []
    for x in individuals:
      i = self.ikb.Individual(gender=gender_table[x[3]])
      if x[1] is None and x[2] is None:
        founders.append(i)

    for i in founders:
      i = self.ikb.save(i)

    for x in individuals:
      if x[1] is None and x[2] is None:
        continue
      if not x[1] is None:
        i_map[x[0]].father = i_map[x[1]]
      if not x[2] is None:
        i_map[x[0]].mother = i_map[x[2]]
      i_map[x[0]] = ikb.save(i_map[x[0]])

    self.individuals.extend(i_map.values())

  def enroll_individuals(self, individuals):
    self.register_individuals(individuals)
    for k, i in enumerate(self.individuals):
      e = self.ikb.Enrollment(study=self.study, individual=i,
                              study_code= '%d' % k)
      self.enrollment.append(self.ikb.save(e))

  def get_device(self, vendor, model, release):
    device = self.skb.Device()
    device.vendor, device.model, device.release = vendor, model, release
    return self.skb.save(device)

  def get_action_setup(self, notes):
    asetup = self.skb.ActionSetup()
    asetup.notes = notes
    return self.skb.save(asetup)

  def get_action_helper(self, aclass, study, target, device, asetup, atype, operator):
    desc = "This is a simulation"
    action = aclass()
    action.setup, action.device, action.actionType = asetup, device, atype
    action.operator, action.context, action.description = operator, study, desc
    action.target = target
    return self.skb.save(action)

  def get_action_on_individual(self, enrollment, device, asetup, atype, operator):
    return self.get_action_helper(self.ikb.ActionOnIndividual,
                                  enrollment.study, enrollment.individual,
                                  device, asetup, atype, operator)

  def get_action_on_sample(self, sample, device, asetup, atype, operator):
    return self.get_action_helper(self.ikb.ActionOnSample,
                                  self.study, sample,
                                  device, asetup, atype, operator)

  def get_action_on_sample_slot(self, sample_slot, device, asetup, atype, operator):
    return self.get_action_helper(self.ikb.ActionOnSampleSlot,
                                  self.study, sample_slot,
                                  device, asetup, atype, operator)

  def get_action_on_data_collection(self, data_collection, device, asetup, atype, operator):
    return self.get_action_helper(self.ikb.ActionOnDataCollection,
                                  self.study, data_collection,
                                  device, asetup, atype, operator)

  def get_action_on_data_collection_item(self, item, device, asetup, atype, operator):
    return self.get_action_helper(self.ikb.ActionOnDataCollectionItem,
                                  self.study, item,
                                  device, asetup, atype, operator)

  def acquire_blood_sample(self, enrollment, device, asetup, atype, operator, volume):
    action = self.get_action_on_individual(enrollment, device, asetup, atype, operator)
    #--
    status = self.sstatus_map['USABLE']
    sample = self.skb.BloodSample()
    sample.action, sample.outcome   = action, self.outcome_map['PASSED']
    sample.labLabel = '%s-%s' % (enrollment.study.label, enrollment.studyCode)
    sample.barcode  = sample.id
    sample.initialVolume = sample.currentVolume = volume
    return self.skb.save(sample)

  def acquire_blood_samples(self, conf):
    device = self.get_device(conf['organization'], conf['department'], '0.0')
    asetup = self.get_action_setup(conf['notes'])
    atype  = self.atype_map('ACQUISITION')

    for e in self.enrollment:
      self.acquire_blood_sample(e, device, asetup, atype, conf['operator'], conf['volume'])

  def extract_dna_sample(self, blood_sample, device, asetup, atype, operator):
    action = self.get_action_on_sample(blood_sample, device, asetup, atype, operator)
    status = self.sstatus_map['USABLE']
    #-
    sample = self.skb.DNASample()
    sample.action, sample.outcome   = action, self.outcome_map['PASSED']
    sample.labLabel = '%s-DNA-%s' % (blood_sample.labLabel, time.time())
    sample.barcode  = sample.id
    sample.initialVolume = sample.currentVolume = 0.1
    sample.qp230260 = sample.qp230280 = 0.3
    return self.skb.save(sample)

  def extract_dna_samples(self):
    device = self.get_device('ACME corp', 'DNA magic extractor', '0.0')
    asetup = self.get_action_setup('nothing to declare')
    atype  = self.atype_map('EXTRACTION')

    for e in self.enrollment:
      blood_sample = self.skb.get_blood_sample(individual=e.individual)
      self.extract_dna_sample(blood_sample, device, asetup, atype, 'Wiley E. Coyote')

  def fill_titer_plate(self, rows, columns, stream):
    plate = self.skb.TiterPlate(rows, columns)
    plate = self.skb.save(plate)
    delta_volume = 0.01
    for r in range(rows):
      for c in range(columns):
        try:
          dna_sample = stream.next()
        except StopIteration:
          return False, plate
        plate_well = self.skb.PlateWell(dna_sample, plate, r, c, delta_volume)
        dna_sample.volume = dna_sample.volume - delta_volume
        dna_sample = self.skb.save(dna_sample)
        plate_well = self.skb.save(plate_well)
    return True, plate

  def fill_titer_plates(self):
    device = self.get_device('Affymetrix', 'GenomeWide 6.0', '0.0')
    asetup = self.get_action_setup('nothing to declare')
    atype  = self.atype_map('PROCESSING')
    rows, columns = 16, 16

    def dna_sample_stream():
      for e in self.enrollment:
        yield self.skb.get_dna_sample(individual=e.individual)

    stream = dna_sample_stream()
    while plates_to_fill:
      plates_to_fill, plate = fill_titer_plate(rows, columns, stream)

  def measure_raw_genotype(self, sample, device, asetup, atype, operator):
    action = self.get_action_on_sample_slot(sample, device, asetup, atype, operator)
    #-
    sample = self.skb.DataSample(name='FIXME-this-is-a-workaround')
    sample.action  = action
    sample.outcome = self.outcome_map['PASSED']
    sample.name = '%s.cel' % sample.id
    sample.dataType = self.dtype_map['GTRAW']
    return self.skb.save(sample)

  def measure_raw_genotypes(self):
    device = self.get_device('Affymetrix', 'GenomeWide 6.0', '0.0')
    asetup = self.get_action_setup('nothing to declare')
    atype  = self.atype_map('PROCESSING')

    for p in self.skb.get_titer_plates():
      for w in self.skb.get_wells_of_plate(p):
        data_sample = self.measure_raw_genotype(w.sample, device, asetup, atype, 'Wiley E. Coyote')
        path = 'file://ELS/els5/storage/a/%s' % data_sample.name
        sha1 = 'a fake sha1 of %s' % data_sample.name
        mime_type = 'x-application/affymetrix-cel' # FIXME, we need to list the legal mime_types
        self.skb.register_data_object(data_sample, mime_type, path, sha1)

  def build_data_collection(self):
    data_collection = self.skb.DataCollection(study=self.study)
    data_collection = self.skb.save(data_collection)
    #-
    for e in self.enrollment:
      data_sample = self.skb.get_dataset(individual=e.individual, dataType=self.dtype_map['GTRAW'])
      data_collection.append_item(dataSample=data_sample)
    #--
    return data_collection

  def call_genotype(self, item, device, asetup, atype,  operator):
    action = self.get_action_on_data_collection_item(item, device, asetup, atype, operator)
    #-
    sample = self.skb.DataSample(name='FIXME-this-is-a-workaround')
    sample.action  = action
    sample.outcome = self.outcome_map['PASSED']
    sample.name = '%s.gt' % sample.id
    sample.dataType = self.dtype_map['GTCALL']
    return self.skb.save(sample)

  def call_genotypes(self):
    device = self.get_device('CRS4', 'MR-birdseed', '0.0')
    asetup = self.get_action_setup('nothing to declare')
    atype  = self.atype_map('PROCESSING')
    selector = "(vendor == ''Affymetrix')&(model== 'GenomeWide 6.0')"
    set_vid = self.gkb.get_snp_markers_set(selector=selector)

    data_collection = self.build_data_collection()
    #-
    for item in data_collection.items():
      data_sample = self.call_genotype(item, device, asetup, atype,  'Wiley E. Coyote')
      (vid, path, sha1, mime_type) = self.gkb.append_gdo(set_vid, probs, confidence, data_sample.action.id)
      self.skb.register_data_object(data_sample.name, mime_type, path, sha1)

def main():
  OME_HOST = os.getenv("OME_HOST", "localhost")
  OME_USER = os.getenv("OME_USER", "root")
  OME_PASS = os.getenv("OME_PASS", "romeo")

  nb = network_builder(OME_HOST, OME_USER, OME_PASS, study_label='foo')
  nb.enroll_individuals(INDIVIDUALS)

  nb.acquire_blood_samples({'organization' : 'Azienda Ospedaliera Brotzu',
                            'department'   : 'Centro trasfusionale',
                            'notes' : 'no notes',
                            'operator' : 'Alfred E. Neuman',
                            'volume' : 100.0})
  nb.extract_dna_samples()
  nb.fill_titer_plates()
  nb.measure_raw_genotypes(vendor='affymetrix', model='GenomeWide6.0')
  nb.call_genotypes()






