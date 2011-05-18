import logging, time
LOG_FILENAME = 'importer.log'
logging.basicConfig(filename=LOG_FILENAME,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level=logging.INFO)

logger = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ch.setFormatter(formatter)

logger.addHandler(ch)

#--------------------------------------------------------------------------------
from bl.vl.sample.kb     import KnowledgeBase as sKB
from bl.vl.individual.kb import KnowledgeBase as iKB

skb = sKB(driver='omero')('biobank05', 'root', 'romeo')
ikb = iKB(driver='omero')('biobank05', 'root', 'romeo')

from bl.vl.sample.kb.drivers.omero.dependency_tree import DependencyTree

gm = ikb.get_gender_table()
gm_by_object = {}
gm_by_object[gm["MALE"].id] = "MALE"
gm_by_object[gm["FEMALE"].id] = "FEMALE"


logger.info('start prefetching enrollment')
study = skb.get_study_by_label('TEST01')
ens = ikb.get_enrolled(study)
logger.info('done prefetching enrollment')

logger.info('start prefetching AffymetrixCel')
dss = skb.get_bio_samples(skb.AffymetrixCel)
logger.info('done prefetching AffymetrixCel')

dt = DependencyTree(skb, ikb, [ikb.Individual, skb.BioSample,
                               skb.PlateWell, skb.DataSample])

for e in ens:
  v = dt.get_connected(e.individual)
  dep = filter(lambda x: type(x) == skb.AffymetrixCel, v)
  if len(dep) == 0:
    continue
  d = dep[0]
  gender = gm_by_object[e.individual.gender.id]
  print e.individual.id, gender, d.label



