"""
A rough example of basic pedigree info generation.
"""

# HARDWIRED TO IMMUNO DATA

import logging, csv, argparse, sys

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.kb.drivers.omero.ehr import EHR
import bl.vl.individual.pedigree as ped


LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

DIAGNOSIS_ARCH = 'openEHR-EHR-EVALUATION.problem-diagnosis.v1'
DIAGNOSIS_FIELD = 'at0002.1'
T1D_ICD10 = 'icd10-cm:E10'
MS_ICD10 = 'icd10-cm:G35'

PLINK_MISSING = -9
PLINK_UNAFFECTED = 0
PLINK_AFFECTED = 1

FIELDS = ["fam_label", "ind_label", "fat_label", "mot_label", "gender",
          "t1d_status", "ms_status"]


def make_parser():
  parser = argparse.ArgumentParser(description='build the first columns of a ped file from VL')
  parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
  parser.add_argument('--loglevel', type=str, choices = LOG_LEVELS,
                      help='logging level', default='INFO')
  parser.add_argument('-H', '--host', type=str, help='omero hostname',
                      default='localhost')
  parser.add_argument('-U', '--user', type=str, help='omero user',
                      default='root')
  parser.add_argument('-P', '--passwd', type=str, help='omero password',
                      required=True)
  parser.add_argument('-S', '--study', type=str, required=True,
                      help='study used to retrieve individuals that will be written to ped file')
  parser.add_argument('--out_file', type=str, help='output file path',
                      required=True)
  return parser

def build_families(individuals, logger):
  not_one_parent = [i for i in individuals if not
                    (((i.mother is None) or (i.father is None)) and
                     not (i.mother is None and i.father is None))]
  logger.info("individuals: %d" % len(individuals))
  logger.info("individuals: with 0 or 2 parents: %d" % len(not_one_parent))
  logger.info("analyzing pedigree")
  founders, non_founders, dangling, couples, children = ped.analyze(
    not_one_parent
    )
  logger.info("splitting into families")
  return ped.split_disjoint(not_one_parent, children)


def main(argv):
  parser = make_parser()
  args = parser.parse_args(argv)

  log_level = getattr(logging, args.loglevel)
  kwargs = {'format': LOG_FORMAT, 'datefmt': LOG_DATEFMT, 'level': log_level}
  if args.logfile:
    kwargs['filename'] = args.logfile
  logging.basicConfig(**kwargs)
  logger = logging.getLogger()
  
  kb = KB(driver='omero')(args.host, args.user, args.passwd)
  logging.debug('Loading all individuals from omero')
  all_inds = kb.get_objects(kb.Individual)  # store all inds to cache
  logging.debug('%d individuals loaded' % len(all_inds))
  study = kb.get_study(args.study)
  if study is None:
    logger.error('Study %s does not exist, stopping program' % args.study)
    sys.exit(2)
  logging.debug('Loading enrolled individuals for study %s' % study.label)
  enrolled = kb.get_enrolled(study)
  logging.debug('%d individuals loaded' % len(enrolled))
  enrolled_map = dict((e.individual.id, (e.studyCode, e.individual))
                      for e in enrolled)
  logging.debug('Loading EHR records')
  ehr_records = kb.get_ehr_records()
  logging.debug('%s EHR records loaded' % len(ehr_records))
  ehr_records_map = {}
  for r in ehr_records:
    ehr_records_map.setdefault(r['i_id'], []).append(r)
  affection_map = {}
  for ind_id, ehr_recs in ehr_records_map.iteritems():
    affection_map[ind_id] = dict(t1d=PLINK_UNAFFECTED, ms=PLINK_UNAFFECTED)
    ehr = EHR(ehr_recs)
    if ehr.matches(DIAGNOSIS_ARCH, DIAGNOSIS_FIELD, T1D_ICD10):
      affection_map[ind_id]['t1d'] = PLINK_AFFECTED
    if ehr.matches(DIAGNOSIS_ARCH, DIAGNOSIS_FIELD, MS_ICD10):
      affection_map[ind_id]['ms'] = PLINK_AFFECTED

  immuno_inds = [i for (ind_id, (st_code, i)) in enrolled_map.iteritems()]
  families = build_families(immuno_inds, logger)
  logger.info("found %d immunochip-related families" % len(families))
  
  def resolve_label(i):
    try:
      return enrolled_map[i.id][0]
    except KeyError:
      return i.id

  def resolve_pheno(i):
    try:
      immuno_affection = affection_map[i.id]
    except KeyError:
      return PLINK_MISSING, PLINK_MISSING
    return immuno_affection["t1d"], immuno_affection["ms"]

  kb.Gender.map_enums_values(kb)
  gender_map = lambda x: 2 if x == kb.Gender.FEMALE else 1

  logger.info("writing miniped")
  with open(args.out_file, "w") as f:
    writer = csv.DictWriter(f, FIELDS, delimiter="\t", lineterminator="\n")
    for k, fam in enumerate(families):
      fam_label = "FAM_%d" % (k+1)
      for i in fam:
        r = {}
        r["fam_label"] = fam_label
        r["ind_label"] = resolve_label(i)
        r["fat_label"] = 0 if i.father is None else resolve_label(i.father)
        r["mot_label"] = 0 if i.mother is None else resolve_label(i.mother)
        r["gender"] = gender_map(i.gender)
        r["t1d_status"], r["ms_status"] = resolve_pheno(i)
        writer.writerow(r)


if __name__ == "__main__":
  main(sys.argv[1:])
