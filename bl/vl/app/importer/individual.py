"""
Import of individuals collections
=================================


Will read in a csv file with the following columns::

  study label gender   father mother
  xxx   id2   male   id4    id5
  xxx   id3   female None   None
  ....

A study with label ``xxx`` will be automatically generated if missing,
and the individuals will be enrolled in the given study. It is not
possible to import the same individual twice: the related file rows
will be noisily ignored.

"""


import itertools as it
import time
import json


class Ind(object):
  """
  An utility class that quacks as expected by import_pedigree
  """
  def __init__(self, label, gender, father, mother):
    self.id = label
    self.gender = gender
    self.father = father
    self.mother = mother

  def is_male(self):
    return self.gender.upper() == 'MALE'

  def is_female(self):
    return self.gender.upper() == 'FEMALE'

  def __str__(self):
    return '%s (%s) [%s, %s]' % (self.id, self.gender,
                                 self.father if self.father else None,
                                 self.mother if self.mother else None)


from bl.vl.individual.pedigree  import import_pedigree
import csv

from core import Core

from version import version
class Recorder(Core):
  """
  An utility class that handles the actual recording into VL
  """
  def __init__(self, out_stream=None, study_label=None,
               host=None, user=None, passwd=None,
               keep_tokens=1, batch_size=1000,
               operator='Alfred E. Neumann',
               action_setup_conf=None,
               logger=None
               ):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   study_label=study_label, logger=logger)
    self.operator = operator
    self.action_setup_conf = action_setup_conf
    self.out_stream = out_stream
    if self.out_stream:
      self.out_stream.writeheader()

    self.individuals_to_be_saved = []
    self.enrollments_to_be_saved = []
    self.chunk_size = batch_size
    self.known_studies = {}
    device = self.get_device('importer-%s' % version,
                             'CRS4', 'IMPORT', version)
    asetup = self.get_action_setup('import-prog-%f' % time.time(),
                                   json.dumps(self.action_setup_conf))
    acat  = self.kb.ActionCategory.IMPORT
    operator = 'Alfred E. Neumann'

    self.action = self.kb.factory.create(self.kb.Action,
                                         {'setup' : asetup,
                                          'device' : device,
                                          'actionCategory' : acat,
                                          'operator' : operator,
                                          'context' : self.default_study,
                                          })

    #-- FIXME what happens if we do not have individuals to save?
    self.action.save()
    #
    self.counter = 0
    #--
    self.known_enrollments = {}
    if self.default_study:
      self.logger.info('start pre-loading known enrolled individuals')
      known_enrollments = self.kb.get_enrolled(self.default_study)
      for e in known_enrollments:
        self.known_enrollments[e.studyCode] = e
      self.logger.info('done pre-loading known enrolled individuals')
      self.logger.info('there are %d enrolled individuals in study %s'
                       % (len(self.known_enrollments),
                          self.default_study.label))
    #--

  def dump_out(self):
    self.logger.debug('\tthere are %s records to save' %
                      len(self.individuals_to_be_saved))
    self.kb.save_array(self.individuals_to_be_saved)
    for i, e in it.izip(self.individuals_to_be_saved,
                        self.enrollments_to_be_saved):
      e.individual = i
    self.kb.save_array(self.enrollments_to_be_saved)

    for i, e in it.izip(self.individuals_to_be_saved, self.enrollments_to_be_saved):
      self.out_stream.writerow({'study' : e.study.label,
                                'label' : e.studyCode,
                                'type' : 'Individual',  'vid' : i.id})

    self.individuals_to_be_saved = []
    self.enrollments_to_be_saved = []
    self.logger.debug('\tdone')


  def clean_up(self):
    self.dump_out()

  def retrieve_enrollment(self, identifier):
    study_label, label = identifier
    self.logger.info('importing (%s, %s)' % (study_label, label))

    assert study_label == self.default_study.label

    if self.default_study and label in self.known_enrollments:
      study = self.default_study
      e = self.known_enrollments[label]
      self.logger.info('using previously loaded enrollment (%s, %s)' %
                       (study_label, label))
    else:
      study = self.default_study if self.default_study \
              else self.known_studies.setdefault(study_label,
                                                 self.get_study(study_label))
      e = self.kb.get_enrollment(study, ind_label=label)
    return study, e


  def retrieve(self, identifier):
    study, e = self.retrieve_enrollment(identifier)
    return e.individual if e else None

  def record(self, identifier, gender, father, mother):
    gender_map = {'MALE' : self.kb.Gender.MALE,
                  'FEMALE' : self.kb.Gender.FEMALE}

    # FIXME quick hack to support reloading...
    if father:
      father.reload()
    if mother:
      mother.reload()
    self.logger.info('importing %s %s %s %s' % (identifier, gender,
                                                father.id if father else None,
                                                mother.id if mother else None))
    study, e = self.retrieve_enrollment(identifier)
    if e:
      self.logger.warn('ignoring %s because it has already been enrolled'
                       % (identifier))

    self.logger.info('creating %s %s %s %s' % (identifier, gender,
                                               father.id if father else None,
                                               mother.id if mother else None))
    conf = {'gender' : gender_map[gender.upper()],
            'action' : self.action}
    if father:
      conf['father'] = father
    if mother:
      conf['mother'] = mother
    i = self.kb.factory.create(self.kb.Individual, conf)
    e = self.kb.factory.create(self.kb.Enrollment,
                               {'study' : self.default_study,
                                'individual' : i,
                                'studyCode': identifier[1]})
    #--
    self.individuals_to_be_saved.append(i)
    self.enrollments_to_be_saved.append(e)
    if len(self.individuals_to_be_saved) >= self.chunk_size:
      self.dump_out()
    return i

  def do_consistency_checks(self, records):
    study_label = records[0]['study']
    seen = {}
    for i, r in enumerate(records):
      bad = 'bad record %d:' % i
      if r['study'] != study_label:
        msg = 'non uniform study label. aborting'
        self.logger.critical(bad + msg)
        raise ValueError(msg)
      if r['gender'].upper() not in ['MALE', 'FEMALE']:
        msg = 'unknown gender value. aborting'
        self.logger.critical(bad + msg)
        raise ValueError(msg)
      seen[r['label']] = r

    for i, r in enumerate(records):
      bad = 'bad record %d:' % i
      for parent in ['father', 'mother']:
        if not (r[parent].upper() == 'NONE'
                or r[parent] in seen
                or r[parent] in self.known_enrollments):
          msg = 'undefined %s label.' % parent
          self.logger.critical(bad + msg)
          raise ValueError(msg)



help_doc = """
import new individual definitions into a virgil system and register
them to a study.
"""
def make_parser_individual(parser):
  parser.add_argument('--study', type=str,
                      help="""Default study to enroll into.
                      It will over-ride the study column value""")
  parser.add_argument('-N', '--batch-size', type=int,
                      help="""Size of the batch of individuals
                      to be processed in parallel (if possible)""",
                      default=1000)


def canonize_records(args, records):
  fields = ['study']
  for f in fields:
    if hasattr(args, f) and getattr(args, f) is not None:
      for r in records:
        r[f] = getattr(args, f)

def import_individual_implementation(logger, args):
  #--
  action_setup_conf = {}
  for x in dir(args):
    if not (x.startswith('_') or x.startswith('func')):
      action_setup_conf[x] = getattr(args, x)
  #FIXME HACKS
  action_setup_conf['ifile'] = action_setup_conf['ifile'].name
  action_setup_conf['ofile'] = action_setup_conf['ofile'].name

  f = csv.DictReader(args.ifile, delimiter='\t')
  records = [r for r in f]
  if len(records) == 0:
    return

  canonize_records(args, records)
  study_label = records[0]['study']

  o = csv.DictWriter(args.ofile, fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t')
  recorder = Recorder(o, study_label, args.host, args.user, args.passwd,
                      args.keep_tokens, args.batch_size,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf, logger=logger)

  recorder.do_consistency_checks(records)

  def istream():
    for r in records:
      k = (r['study'], r['label'])
      i = Ind(k, r['gender'],
              None if r['father'] == 'None' else (r['study'], r['father']),
              None if r['mother'] == 'None' else (r['study'], r['mother']))
      yield i
  import_pedigree(recorder, istream())
  recorder.clean_up()


def do_register(registration_list):
  registration_list.append(('individual', help_doc,
                            make_parser_individual,
                            import_individual_implementation))

