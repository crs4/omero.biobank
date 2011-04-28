"""
Import of individuals collections
,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,


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

from bl.vl.individual.pedigree  import import_pedigree

from core import Core

import csv

#-----------------------------------------------------------------------------
#FIXME this should be factored out....

import logging, time
logger = logging.getLogger(__name__)
counter = 0
def debug_wrapper(f):
  def debug_wrapper_wrapper(*args, **kv):
    global counter
    now = time.time()
    counter += 1
    logger.debug('%s[%d] in' % (f.__name__, counter))
    res = f(*args, **kv)
    logger.debug('%s[%d] out (%f)' % (f.__name__, counter, time.time() - now))
    counter -= 1
    return res
  return debug_wrapper_wrapper
#-----------------------------------------------------------------------------

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

class Recorder(Core):
  """
  An utility class that handles the actual recording into VL
  """
  def __init__(self, study_label=None, host=None, user=None, passwd=None):
    super(Recorder, self).__init__(host, user, passwd)
    self.default_study = None
    if study_label:
      s = self.skb.get_study_by_label(study_label)
      if not s:
        s = self.skb.save(self.skb.Study(label=study_label))
      self.default_study = s
    self.known_studies = {}
    self.device = self.get_device('importer-0.0', 'CRS4', 'IMPORT', '0.0')
    self.asetup = self.get_action_setup('import-prog-%f' % time.time(),
                                        # FIXME the json below should
                                        # record the app version, and the
                                        # parameters used.  unclear if we
                                        # need to register the file we load
                                        # data from, since it is, most
                                        # likely, a transient object.
                                        '{"foo2": "foo"}')
    self.acat  = self.acat_map['IMPORT']
    # FIXME this should be detected from the env.
    self.operator = 'Alfred E. Neumann'
    #
    self.input_rows = {}
    self.counter = 0

  @debug_wrapper
  def create_import_action(self, study, description=''):
    return self.create_action_helper(self.skb.Action, description,
                                     study, self.device,
                                     self.asetup, self.acat, self.operator)

  @debug_wrapper
  def retrieve(self, identifier):
    study_label, label = identifier
    study = self.default_study if self.default_study \
            else self.known_studies.setdefault(study_label,
                                               self.get_study_by_label(study_label))
    e = self.ikb.get_enrollment(study_label=study.label, ind_label=label)
    if e:
      logger.info('using (%s,%s) already in kb' % (study.label, label))
    return e.individual if e else None

  @debug_wrapper
  def record(self, identifier, gender, father, mother):
    logger.debug('\tworking on  %s %s %s %s' % (identifier, gender, father, mother))
    logger.info('importing %s %s %s %s' % (identifier, gender, father, mother))
    study_label, label = identifier
    study = self.default_study if self.default_study \
            else self.known_studies.setdefault(study_label,
                                               self.get_study_by_label(study_label))
    e = self.ikb.get_enrollment(study_label=study.label, ind_label=label)
    if not e:
      logger.info('creating %s %s %s %s' % (identifier, gender, father, mother))
      action = self.create_import_action(study,
                                         description=self.input_rows[identifier])
      i = self.ikb.Individual(gender=self.gender_map[gender.upper()])
      i.action = action
      if father:
        # FIXME: this is disabled, since father does not automatically load field objects
        # if not father.gender == self.gender_map['MALE']:
        #   raise ValueError('putative father of %s is not male' % label)
        i.father = father
      if mother:
        # FIXME: this is disabled, since father does not automatically load field objects
        # if not mother.gender == self.gender_map['FEMALE']:
        #   raise ValueError('putative mother of %s is not female' % label)
        i.mother = mother
      i = self.ikb.save(i)
      e = self.ikb.Enrollment(study=study, individual=i,
                              study_code=label)
      e = self.ikb.save(e)
    return e.individual

help_doc = """
import new individual definitions into a virgil system and register
them to a study.
"""
def make_parser_individual(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""Default study to enroll into.
                      It will over-ride the study column value""")

def import_individual_implementation(args):
  recorder = Recorder(args.study, args.host, args.user, args.passwd)
  def istream(f, input_rows):
    for r in f:
      k = (r['study'], r['label'])
      assert not input_rows.has_key(k)
      input_rows[k] = '%s' % r
      i = Ind(k, r['gender'],
              None if r['father'] == 'None' else (r['study'], r['father']),
              None if r['mother'] == 'None' else (r['study'], r['mother']))
      yield i
  f = csv.DictReader(args.ifile, delimiter='\t')
  import_pedigree(recorder, istream(f, recorder.input_rows))

def do_register(registration_list):
  registration_list.append(('individual', help_doc,
                            make_parser_individual,
                            import_individual_implementation))

