"""
Select a group of individuals
=============================

It will select a group of individuals from a specific group (from all
avalable individuals, if no group is selected). The selection is
controlled by the following parameters:

  * total number of individuals selected
  * male fraction
  * reference disease
  * control fraction
  * presence of specific datasets

.. todo::

   defining what we mean by presence of specific datasets will be a mess...

The basic command is the following::

  usage: kb_query selector [-h] [--study STUDY] --group-label GROUP_LABEL
                           --total-number TOTAL_NUMBER --male-fraction
                           MALE_FRACTION --reference-disease REFERENCE_DISEASE
                           --control-fraction CONTROL_FRACTION
                           [--required-datasample {AffymetrixCel,IlluminaBeadChipAssay}]
                           [--seed SEED]

  optional arguments:
    -h, --help            show this help message and exit
    --study STUDY         will select individuals from this study
    --group-label GROUP_LABEL
                          the new group label
    --total-number TOTAL_NUMBER
                          total number of individuals required
    --male-fraction MALE_FRACTION
                          required fraction of male individuals
    --reference-disease REFERENCE_DISEASE
                          the coded text (e.g., icd10-cm:E10) identifying the
                          reference disease
    --control-fraction CONTROL_FRACTION
                          required fraction of control individuals
    --required-datasample {AffymetrixCel,IlluminaBeadChipAssay}
                          required datasample type
    --seed SEED           random seed (will default to int(time.time())


The results will be presented as a file that can be used to generate a
new study (FIXME, we should have the concept of a group that is
independent of a study.). The file will have the following columns::

    study label individual
    XXX   0001  V20940239409
    XXX   0002  V20940239509
    XXX   0003  V20940239609
    XXX   0004  V20940239709
    ...

  where study is the name of the new study

"""

from bl.vl.kb.dependency import DependencyTree

from bl.vl.app.importer.core import Core
from version import version

import random

import csv, json
import time, sys
import itertools as it


import logging

class Selector(Core):
  """
  An utility class that handles the selection of groups of individuals
  and presents the results as a file that can be used to generate a
  new study (FIXME, we should have the concept of a group that is
  independent of a study.)

  It will output a file with the following columns::

    study label individual
    XXX   0001  V20940239409
    XXX   0002  V20940239509
    XXX   0003  V20940239609
    XXX   0004  V20940239709
    ...

  where study is the name of the new study

  """
  SUPPORTED_DATA_SAMPLE_TYPES = ['AffymetrixCel', 'IlluminaBeadChipAssay']

  def __init__(self, host=None, user=None, passwd=None,
               operator='Alfred E. Neumann', logger=None):
    """
    FIXME
    """
    super(Selector, self).__init__(host, user, passwd, logger=logger)

    #FIXME we need to do this to sync with the DB idea of the enums.
    self.kb.Gender.map_enums_values(self.kb)
    self.kb.AffymetrixCelArrayType.map_enums_values(self.kb)
    self.kb.IlluminaBeadChipAssayType.map_enums_values(self.kb)

  def dump(self, args, ots):
    if args.seed is None:
      random.seed()
    else:
      random.seed(args.seed)

    if args.study is None:
      individuals = self.kb.get_objects(self.kb.Individual)
    else:
      study = self.kb.get_study(args.study)
      if not study:
        self.logger.critical('study %s does not exist' % args.study)
        sys.exit(1)
      individuals = [e.individual for e in self.kb.get_enrolled(study)]

    if args.required_datasample:
      tech = getattr(self.kb, args.required_datasample)
      self.logger.info('start loading dependency tree')
      dt = DependencyTree(self.kb)
      self.logger.info('done loading dependency tree')
    else:
      tech = None

    # find all eligible individuals
    atype_control   = 'openEHR-EHR-EVALUATION.exclusion-problem_diagnosis.v1'
    atype_diagnosis = 'openEHR-EHR-EVALUATION.problem-diagnosis.v1'
    field = 'at0002.1'
    male_controls = []
    male_affected = []
    female_controls = []
    female_affected = []

    self.logger.debug('reference_disease: %s'  % args.reference_disease)
    for i in individuals:
      if tech:
        v = dt.get_connected(i, tech)
        if not v:
          continue
      ehr = self.kb.get_ehr(i)
      self.logger.debug('ehr: %s'  % ehr.recs)
      if ehr.matches(atype_control):
        if i.gender == self.kb.Gender.MALE:
          male_controls.append(i)
        else:
          female_controls.append(i)
      elif ehr.matches(atype_diagnosis, field, args.reference_disease):
        if i.gender == self.kb.Gender.MALE:
          male_affected.append(i)
        else:
          female_affected.append(i)
    # FIXME: no checks for round-off effects
    total_affected = int((1.0 - args.control_fraction) * args.total_number)
    total_male_affected   = int(total_affected * args.male_fraction)
    total_female_affected = int(total_affected * (1.0 - args.male_fraction))

    total_controls = int(args.control_fraction * args.total_number)
    total_male_controls   = int(total_controls * args.male_fraction)
    total_female_controls = int(total_controls * (1.0 - args.male_fraction))

    if total_male_affected > len(male_affected):
      self.logger.critical('Requested %d affected males out of %d'
                           % (total_male_affected, len(male_affected)))
      sys.exit(1)
    if total_female_affected > len(female_affected):
      self.logger.critical('Requested %d affected females out of %d'
                           % (total_female_affected, len(female_affected)))
      sys.exit(1)

    if total_male_controls > len(male_controls):
      self.logger.critical('Requested %d controls males out of %d'
                           % (total_male_controls, len(male_controls)))
      sys.exit(1)
    if total_female_controls > len(female_controls):
      self.logger.critical('Requested %d controls females out of %d'
                           % (total_female_controls, len(female_controls)))
      sys.exit(1)

    s_male_controls   = random.sample(male_controls, total_male_controls)
    self.logger.info('selected %d male controls out of %d' %
                     (len(s_male_controls), len(male_controls)))
    s_female_controls = random.sample(female_controls, total_female_controls)
    self.logger.info('selected %d female controls out of %d' %
                     (len(s_female_controls), len(female_controls)))

    s_male_affected   = random.sample(male_affected, total_male_affected)
    self.logger.info('selected %d male affected out of %d' %
                     (len(s_male_affected), len(male_affected)))
    s_female_affected = random.sample(female_affected, total_female_affected)
    self.logger.info('selected %d female affected out of %d' %
                     (len(s_female_affected), len(female_affected)))

    all_individuals = (s_male_controls + s_female_controls +
                       s_male_affected + s_female_affected)

    for i, indy in enumerate(all_individuals):
      ots.writerow({
        'group' : args.group_label,
        'group_code' : 'I%04d' % i,
        'individual' : indy.id,
        })

#-------------------------------------------------------------------------
help_doc = """
Select a group of individuals
"""

def make_parser_selector(parser):
  def a_fraction_of_one(s):
    x = float(s)
    if 0.0 <= x <= 1.0:
      return x
    raise ValueError()
  def coded_text(s):
    parts = s.split(':')
    if len(parts) != 2:
      raise ValueError()
    return s
  def positive_int(s):
    i = int(s)
    if i < 1:
      raise ValueError()
    return i
  parser.add_argument('--study', type=str,
                      help="will select individuals from this study")
  parser.add_argument('--group-label', type=str,
                      required=True,
                      help="the new group label")
  parser.add_argument('--total-number', type=positive_int,
                      required=True,
                      help="total number of individuals required")
  parser.add_argument('--male-fraction', type=a_fraction_of_one,
                      required=True,
                      help="required fraction of male individuals")
  parser.add_argument('--reference-disease', type=coded_text,
                      required=True,
                      help="""the coded text (e.g., icd10-cm:E10) identifying
                      the reference disease""")
  parser.add_argument('--control-fraction', type=a_fraction_of_one,
                      required=True,
                      help="required fraction of control individuals")
  parser.add_argument('--required-datasample', type=str,
                      choices=Selector.SUPPORTED_DATA_SAMPLE_TYPES,
                      help="required datasample type")
  parser.add_argument('--seed', type=int,
                      help="random seed (will default to int(time.time())")

def import_selector_implementation(logger, args):
  #--
  selector = Selector(host=args.host, user=args.user, passwd=args.passwd,
                             logger=logger)
  fieldnames = ['group', 'group_code', 'individual']
  otsv = csv.DictWriter(args.ofile, fieldnames, delimiter='\t')
  otsv.writeheader()
  selector.dump(args, otsv)

def do_register(registration_list):
  registration_list.append(('selector', help_doc,
                            make_parser_selector,
                            import_selector_implementation))


