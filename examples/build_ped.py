"""
A rough example of ped/map generation.  Several things are hardwired.
"""

import sys, argparse, logging

from bl.vl.kb import KnowledgeBase as KB
import bl.vl.individual.pedigree as ped
from bl.vl.genotype.io import PedWriter

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']


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


def make_parser():
  parser = argparse.ArgumentParser(description="build ped/map files from VL")
  parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
  parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                      help='logging level', default='INFO')
  parser.add_argument('--marker-set', type=str, help='marker set label',
                      required=True)
  parser.add_argument('--prefix', type=str, help='output files prefix',
                      default='bl_vl_gt')
  parser.add_argument('-H', '--host', type=str, help='omero hostname',
                      default='localhost')
  parser.add_argument('-U', '--user', type=str, help='omero user',
                      default='root')
  parser.add_argument('-P', '--passwd', type=str, help='omero password',
                      required=True)
  return parser


def main(argv):
  parser = make_parser()
  args = parser.parse_args(argv)

  log_level = getattr(logging, args.loglevel)
  kwargs = {'format': LOG_FORMAT, 'datefmt': LOG_DATEFMT, 'level': log_level}
  if args.logfile:
    kwargs['filename'] = args.logfile
  logging.basicConfig(**kwargs)
  logger = logging.getLogger()

  kb = KB(driver="omero")(args.host, args.user, args.passwd)
  logger.info("getting data samples")
  ms = kb.get_snp_markers_set(label=args.marker_set)
  if ms is None:
    msg = "marker set %s not present in kb, bailing out" % args.marker_set
    logger.critical(msg)
    sys.exit(msg)
  query = "from GenotypeDataSample g where g.snpMarkersSet.id = :id"
  params = {"id": ms.omero_id}
  gds = kb.find_all_by_query(query, params)
  logger.info("found %d data samples for marker set %s" %
              (len(gds), args.marker_set))
  logger.info("updating dep tree")
  kb.update_dependency_tree()
  individuals = [get_individual(kb, ds) for ds in gds]
  ds_by_ind_id = dict((i.id, ds) for i, ds in zip(individuals, gds))
  logger.info("getting families")
  families = get_all_families(kb)
  ped_writer = PedWriter(ms, base_path=args.prefix)
  logger.info("writing map file")
  ped_writer.write_map()
  logger.info("writing ped file")
  for i, fam in enumerate(families):
    if set(ds_by_ind_id.get(i.id) for i in fam) != set([None]):
      fam_label = "FAM_%d" % (i+1)
      logger.info("writing family %s" % fam_label)
      ped_writer.write_family(fam_label, fam, ds_by_ind_id)
  logger.info("all finished")


if __name__ == "__main__":
  main(sys.argv[1:])
