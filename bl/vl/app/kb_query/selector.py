# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Select a group of individuals
=============================

Select a group of individuals from a specific group (from all avalable
individuals, if no group is selected). Selections are controlled by
the following parameters:

  * total number of individuals selected
  * male fraction
  * reference disease
  * control fraction
  * presence of specific datasets

.. todo::

   explain what we mean by 'presence of specific datasets'.

The output file has the following format::

    study label individual
    XXX   0001  V20940239409
    XXX   0002  V20940239509
    XXX   0003  V20940239609
    XXX   0004  V20940239709
    ...

where 'study' is the name of the new study. The file can be used to
generate a new study.

.. todo::

  clarify the differences between 'group' and 'study'.
"""

import os, random, csv

from bl.vl.kb.dependency import DependencyTree
from bl.vl.app.importer.core import Core


class Selector(Core):

  SUPPORTED_DATA_SAMPLE_TYPES = ['AffymetrixCel', 'IlluminaBeadChipAssay']

  def __init__(self, host=None, user=None, passwd=None,
               operator='Alfred E. Neumann', logger=None):
    super(Selector, self).__init__(host, user, passwd, logger=logger)
    # sync with the DB vision of the enums
    self.kb.Gender.map_enums_values(self.kb)
    self.kb.AffymetrixCelArrayType.map_enums_values(self.kb)
    self.kb.IlluminaBeadChipAssayType.map_enums_values(self.kb)

  def __critical(self, msg):
    self.logger.critical(msg)
    raise ValueError(msg)

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
        self.__critical('study %s does not exist' % args.study)
      individuals = [e.individual for e in self.kb.get_enrolled(study)]
    if args.required_datasample:
      tech = getattr(self.kb, args.required_datasample)
      self.logger.info('start loading dependency tree')
      dt = DependencyTree(self.kb)
      self.logger.info('done loading dependency tree')
    else:
      tech = None
    atype_control = 'openEHR-EHR-EVALUATION.exclusion-problem_diagnosis.v1'
    atype_diagnosis = 'openEHR-EHR-EVALUATION.problem-diagnosis.v1'
    field = 'at0002.1'
    male_controls = []
    male_affected = []
    female_controls = []
    female_affected = []
    self.logger.debug('reference_disease: %s' % args.reference_disease)
    for i in individuals:
      if tech:
        v = dt.get_connected(i, tech)
        if not v:
          continue
      ehr = self.kb.get_ehr(i)
      self.logger.debug('ehr: %s' % ehr.recs)
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
    # TODO add checks for round-off effects
    total_affected = int((1.0 - args.control_fraction) * args.total_number)
    total_male_affected = int(total_affected * args.male_fraction)
    total_female_affected = int(total_affected * (1.0 - args.male_fraction))
    total_controls = int(args.control_fraction * args.total_number)
    total_male_controls = int(total_controls * args.male_fraction)
    total_female_controls = int(total_controls * (1.0 - args.male_fraction))
    if total_male_affected > len(male_affected):
      self.__critical('Requested %d affected males out of %d' %
                      (total_male_affected, len(male_affected)))
    if total_female_affected > len(female_affected):
      self.__critical('Requested %d affected females out of %d' %
                      (total_female_affected, len(female_affected)))
    if total_male_controls > len(male_controls):
      self.__critical('Requested %d controls males out of %d' %
                      (total_male_controls, len(male_controls)))
    if total_female_controls > len(female_controls):
      self.__critical('Requested %d controls females out of %d' %
                      (total_female_controls, len(female_controls)))
    s_male_controls = random.sample(male_controls, total_male_controls)
    self.logger.info('selected %d male controls out of %d' %
                     (len(s_male_controls), len(male_controls)))
    s_female_controls = random.sample(female_controls, total_female_controls)
    self.logger.info('selected %d female controls out of %d' %
                     (len(s_female_controls), len(female_controls)))
    s_male_affected = random.sample(male_affected, total_male_affected)
    self.logger.info('selected %d male affected out of %d' %
                     (len(s_male_affected), len(male_affected)))
    s_female_affected = random.sample(female_affected, total_female_affected)
    self.logger.info('selected %d female affected out of %d' %
                     (len(s_female_affected), len(female_affected)))
    all_individuals = (s_male_controls + s_female_controls +
                       s_male_affected + s_female_affected)
    for i, indy in enumerate(all_individuals):
      ots.writerow({
        'group': args.group_label,
        'group_code': 'I%04d' % i,
        'individual': indy.id,
        })


help_doc = """
Select a group of individuals
"""


def make_parser(parser):
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
  parser.add_argument('--study', metavar="STRING",
                      help="select individuals from this study")
  parser.add_argument('--group-label', metavar="STRING", required=True,
                      help="new group label")
  parser.add_argument('--total-number', type=positive_int, metavar="INT",
                      required=True, help="total number of individuals")
  parser.add_argument('--male-fraction', type=a_fraction_of_one,
                      metavar="FLOAT", required=True,
                      help="fraction of male individuals")
  parser.add_argument('--control-fraction', type=a_fraction_of_one,
                      metavar="FLOAT", required=True,
                      help="fraction of control individuals")
  parser.add_argument('--reference-disease', type=coded_text, metavar="X:Y",
                      required=True, help="disease id (e.g., icd10-cm:E10)")
  parser.add_argument('--required-datasample', metavar="STRING",
                      choices=Selector.SUPPORTED_DATA_SAMPLE_TYPES,
                      help="data sample type, e.g., AffymetrixCel")
  parser.add_argument('--seed', type=int, metavar="INT", help="random seed")


def implementation(logger, args):
  selector = Selector(host=args.host, user=args.user, passwd=args.passwd,
                      logger=logger)
  otsv = csv.DictWriter(args.ofile, ['group', 'group_code', 'individual'],
                        delimiter='\t', lineterminator=os.linesep)
  otsv.writeheader()
  selector.dump(args, otsv)


def do_register(registration_list):
  registration_list.append(('selector', help_doc, make_parser, implementation))
