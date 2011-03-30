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

# o = omero.model.OriginalFileI()

# o.mimetype = 'x-omero-bl/affymetrix-cel'
# o.path = 'hdfs://host:93090/data/foo.cel'

# o.name = <object-vid>
# o.mimetype = 'x-omero-bl/<marker-set-vid>'
# o.path = 'hf:<gdo-table-name>:<rowid>'

INDIVIDUALS = [(0, None, None, 'MALE'), (1, None, None, 'FEMALE'),
               (2, 0, 1, 'MALE'), (3, 0, 1, 'FEMALE')]


def enroll_to_study(ikb, study, individuals):
  for i in individuals:
    e = ikb.Enrollment()
    e.study      = study
    e.individual = i
    e = ikb.save(e)

def bio_sample_loader(skb, atype_map, outcome_map, sstatus_map,
                      study, sample):
  a = skb.Action()
  a.actionType = atype_map['ACQUISITION']
  a.operator = 'Alfred E. Neumann'
  a.context  = study
  a = skb.save(a)
  sample.outcome, sample.action = outcome_map['PASSED'], a
  sample.labLabel = 'lab-label-%s' % time.time()
  sample.barcode  = 'lab-barcode-%s' % time.time()
  sample.initialVolume = 1.0
  sample.currentVolume = 1.0
  sample.status = sstatus_map['USABLE']

def register_blood_samples(skb, study, individuals):
  for i in individuals:
    bs = skb.BloodSample()
    bio_sample_loader(skb, study, i, bs)
    bs = dkb.save(bs)

def register_dna_samples(dkb, blood_samples):
  for bs in blood_samples:
    ds = dkb.DnaSample()
    # fill details
    ds = dkb.save(ds)
    dbk.link(bs, ds, op='')

def load_microtiter_plates(dkb, dna_samples):
  N_WELLS = 96
  if not dna_samples:
    return []
  # fill details
  plates = []
  plate = None
  for i, ds in enumerate(dna_samples):
    j = i % N_WELLS
    if j == 0:
      if plate:
        plates.append(dkb.save(plate))
      plate = dkb.Microtiter()
    plate.fill_well(j, ds)
  return plates

def run_genotyping(dkb, plates):
  pass

def run_birdseed(dkb, cel_results):
  pass

def dump_network(dkb, inds):
  for i in inds:
    print 'Individual: %s' % i.vid
    res = dkb.get_results_for_individual(i.vid)
    for r in res:
      print '\tid:%s' % r.vid
      print '\ttype: %s' % r.type
      for c in dkb.get_data_containers_for_result(r.vid):
        print '\tmimetype: %s' % r.mimetype
        print '\tpath: %s'     % r.path


class network_builder(object):
  def __init__(self, host, user, passwd, study_label):
    self.skb = sKB(driver='omero')(host, user, passwd)
    self.ikb = iKB(driver='omero')(host, user, passwd)
    self.gkb = gKB(driver='omero')(host, user, passwd)
    self.atype_map   = self.skb.get_action_type_table()
    self.outcome_map = self.skb.get_result_outcome_table()
    self.sstatus_map = self.skb.get_sample_status_table()
    self.gender_map = self.ikb.get_gender_table()

    s = self.skb.Study(label=study_label)
    self.study = self.skb.save(s)

  def register_individuals(self, individuals):
    """
    Create and register a group of individuals.
    """
    gender_table = ikb.get_gender_table()
    i_map = {}
    for x in individuals:
      i_map[x[0]] = self.ikb.Individual(gender=gender_table[x[3]])

    for x in individuals:
      if not x[1] is None:
        i_map[x[0]].father = i_map[x[1]]
      if not x[2] is None:
        i_map[x[0]].mother = i_map[x[2]]
      i_map[x[0]] = ikb.save(i_map[x[0]])

    self.individuals = i_map.values()



  def enroll_individuals(self, individuals):
    self.register_individuals(individuals)
    for i in self.individuals:





def main():
  OME_HOST = os.getenv("OME_HOST", "localhost")
  OME_USER = os.getenv("OME_USER", "root")
  OME_PASS = os.getenv("OME_PASS", "romeo")

  nb = network_builder(OME_HOST, OME_USER, OME_PASS, study_label='foo')
  nb.enroll_individuals(INDIVIDUALS)
  nb.get_blood_samples()
  nb.extract_dna_samples()
  nb.get_raw_genotype_data(vendor='affymetrix', model='GenomeWide6.0')
  nb.call_genotypes()


  #--
  blood_samples = register_blood_samples(skb, atype_map, s, inds)
  dna_samples = register_dna_samples(skb, blood_samples)
  #--
  plates      = load_microtiter_plates(skb, dna_samples)
  cel_results = run_genotyping(plates)
  #--
  run_birdseed(cel_results)
  #--
  dump_network(dkb, inds)







