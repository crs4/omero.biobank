"""
A rough example of ped/map generation.  Several things are hardwired.
"""

import logging

from bl.vl.kb import KnowledgeBase as KB
import bl.vl.individual.pedigree as ped
from bl.vl.genotype.io import PedWriter

LOG_LVL = logging.DEBUG
LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'

OME_HOST, OME_USER, OME_PASSWD = "biobank09.crs4.it", "galaxy", "galaxy"
MSET_LABEL = "IMMUNO_BC_11419691_B"


def get_individual(kb, ds):
  individuals = kb.dt.get_connected(ds, kb.Individual)
  assert len(individuals) == 1
  return individuals[0]


def get_all_families(kb):
  inds = kb.get_objects(kb.Individual)
  not_one_parent = [i for i in inds if not
                    (((i.mother is None) or (i.father is None)) and
                     not (i.mother is None and i.father is None))
                    ]
  founders, non_founders, dangling, couples, children = ped.analyze(
    not_one_parent
    )
  return ped.split_disjoint(not_one_parent, children)


if __name__ == "__main__":
  logging.basicConfig(level=LOG_LVL, format=LOG_FORMAT, datefmt=LOG_DATEFMT)
  logger = logging.getLogger()
  kb = KB(driver="omero")(OME_HOST, OME_USER, OME_PASSWD)
  logger.info("getting data samples")
  ms = kb.get_snp_markers_set(label=MSET_LABEL)
  query = "from GenotypeDataSample g where g.snpMarkersSet.id = :id"
  params = {"id": ms.omero_id}
  gds = kb.find_all_by_query(query, params)
  logger.info("found %d data samples for mset %s" % (len(gds), MSET_LABEL))
  logger.info("updating dep tree")
  kb.update_dependency_tree()
  individuals = [get_individual(kb, ds) for ds in gds]
  ds_by_ind_id = dict((i.id, ds) for i, ds in zip(individuals, gds))
  logger.info("getting families")
  families = get_all_families(kb)
  ped_writer = PedWriter(ms)
  logger.info("writing map file")
  ped_writer.write_map()
  logger.info("writing ped file")
  for i, fam in enumerate(families):
    if set(ds_by_ind_id.get(i.id) for i in fam) != set([None]):
      fam_label = "FAM_%d" % (i+1)
      logger.info("writing family %s" % fam_label)
      ped_writer.write_family(fam_label, fam, ds_by_ind_id)
  logger.info("all finished")
