"""
Extract global stats from KB
============================

FIXME

"""

from bl.vl.kb.dependency import DependencyTree

from bl.vl.app.importer.core import Core
from version import version

import csv, json
import time, sys
import itertools as it

import logging

class GlobalStats(Core):
  """
  An utility class that handles the dumping of global stats from KB.
  It will output a tsv files with the following columns::

    study  diagnosis technologies  gender  counts
    ASTUDY icd10:E10 Affymetrix.GENOMEWIDESNP_6;Illumina.HUMAN1M_DUO  MALE   230
    ASTUDY icd10:E10 Affymetrix.GENOMEWIDESNP_6;Illumina.HUMAN1M_DUO  FEMALE 200
    ...

  """

  def __init__(self, host=None, user=None, passwd=None,
               operator='Alfred E. Neumann', logger=None):
    """
    FIXME
    """
    super(GlobalStats, self).__init__(host, user, passwd, logger=logger)

    #FIXME we need to do this to sync with the DB idea of the enums.
    self.kb.Gender.map_enums_values(self.kb)
    self.kb.AffymetrixCelArrayType.map_enums_values(self.kb)
    self.kb.IlluminaBeadChipAssayType.map_enums_values(self.kb)

  def dump(self, study_label, ofile):
    if study_label is None:
      individuals = self.kb.get_objects(self.kb.Individual)
    else:
      study = self.kb.get_study(study_label)
      if not study:
        self.logger.critical('study %s does not exist' % study_label)
        sys.exit(1)
      individuals = [e.individual for e in self.kb.get_enrolled(study)]

    dt = DependencyTree(self.kb)
    self.dump_indiduals(dt, study_label if study_label else '_ALL_',
                        individuals, ofile)

  def dump_indiduals(self, dt, study_label, individuals, ots):
    counts = {}
    for i in individuals:
      key = self.get_profile_of_individual(dt, i)
      counts[key] = counts.get(key, 0) + 1
    for k, v in counts.iteritems():
      ots.writerow({
        'study' : study_label,
        'diagnosis': k[0],
        'technologies': k[1],
        'gender' : k[2],
        'counts' : v
        })

  def get_profile_of_individual(self, dt, individual):
    gender_label = self.__get_label_of_enum(individual.gender)
    v = dt.get_connected(individual)
    technologies = []
    for o in v:
      if isinstance(o, self.kb.AffymetrixCel):
        technologies.append(self.kb.AffymetrixCel.get_ome_table()
                            + self.__get_label_of_enum(o.arrayType))
      elif isinstance(o, self.kb.IlluminaBeadChipAssay):
        technologies.append(self.kb.IlluminaBeadChipAssay.get_ome_table()
                            + self.__get_label_of_enum(o.assayType))

    ehr = self.kb.get_ehr(individual)

    diagnosis = []
    for atype in ['openEHR-EHR-EVALUATION.problem-diagnosis.v1',
                  'openEHR-EHR-EVALUATION.exclusion-problem_diagnosis.v1']:
      field = 'at0002.1'
      diagnosis += [x[1] for x in ehr.get_field_values(atype, field)]

    key = (';'.join(diagnosis), ';'.join(technologies), gender_label)
    return key

  def __get_label_of_enum(self, e):
    def get_label_of_enum(klass, e):
      for g in klass.__enums__:
        if g == e:
          return g.enum_label()

    for klass in [self.kb.Gender, self.kb.AffymetrixCelArrayType,
                  self.kb.IlluminaBeadChipAssayType]:
      if isinstance(e, klass):
        return get_label_of_enum(klass, e)
    else:
      assert False

#-------------------------------------------------------------------------
help_doc = """
Extract global stats from KB in tabular form.
"""

def make_parser_global_stats(parser):
  parser.add_argument('--study', type=str,
                      help="study label")

def import_global_stats_implementation(logger, args):
  #--
  global_stats = GlobalStats(host=args.host, user=args.user, passwd=args.passwd,
                             logger=logger)
  fieldnames = ['study', 'diagnosis', 'technologies', 'gender', 'counts']
  otsv = csv.DictWriter(args.ofile, fieldnames, delimiter='\t')
  otsv.writeheader()
  global_stats.dump(args.study, otsv)

def do_register(registration_list):
  registration_list.append(('global_stats', help_doc,
                            make_parser_global_stats,
                            import_global_stats_implementation))


