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

#-----------------------------------------------------------------------------
#FIXME this should be factored out....

import itertools as it
import logging, time
logger = logging.getLogger()
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

  def __str__(self):
    return '%s (%s) [%s, %s]' % (self.id, self.gender,
                                 self.father if self.father else None,
                                 self.mother if self.mother else None)


from bl.vl.individual.pedigree  import import_pedigree
import csv

from core import Core

class Recorder(Core):
  """
  An utility class that handles the actual recording into VL
  """
  def __init__(self, study_label=None, host=None, user=None, passwd=None,
               keep_tokens=1, batch_size=1000):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens)
    self.default_study = None
    if study_label:
      self.default_study = self.get_study(study_label)

    self.individuals_to_be_saved = []
    self.enrollments_to_be_saved = []
    self.chunk_size = batch_size
    self.known_studies = {}
    device = self.get_device('importer-0.0', 'CRS4', 'IMPORT', '0.0')
    asetup = self.get_action_setup('import-prog-%f' % time.time(),
                                   # FIXME the json below should
                                   # record the app version, and the
                                   # parameters used.  unclear if we
                                   # need to register the file we load
                                   # data from, since it is, most
                                   # likely, a transient object.
                                   '{"foo2": "foo"}')
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
    self.input_rows = {}
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

  @debug_wrapper
  def dump_out(self):
    self.logger.debug('\tthere are %s records to save' %
                      len(self.individuals_to_be_saved))
    self.kb.save_array(self.individuals_to_be_saved)
    for i, e in it.izip(self.individuals_to_be_saved,
                        self.enrollments_to_be_saved):
      e.individual = i
    self.kb.save_array(self.enrollments_to_be_saved)
    self.individuals_to_be_saved = []
    self.enrollments_to_be_saved = []
    self.logger.debug('\tdone')


  @debug_wrapper
  def clean_up(self):
    self.dump_out()

  @debug_wrapper
  def retrieve_enrollment(self, identifier):
    study_label, label = identifier
    self.logger.info('importing (%s, %s)' % (study_label, label))

    assert study_label == self.default_study.label

    if self.default_study and self.known_enrollments.has_key(label):
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


  @debug_wrapper
  def retrieve(self, identifier):
    study, e = self.retrieve_enrollment(identifier)
    return e.individual if e else None

  @debug_wrapper
  def record(self, identifier, gender, father, mother):
    gender_map = {'MALE' : self.kb.Gender.MALE,
                  'FEMALE' : self.kb.Gender.FEMALE}

    self.logger.info('importing %s %s %s %s' % (identifier, gender,
                                                father.id if father else None,
                                                mother.id if mother else None))
    study, e = self.retrieve_enrollment(identifier)
    if e:
      return
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

help_doc = """
import new individual definitions into a virgil system and register
them to a study.
"""
def make_parser_individual(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""Default study to enroll into.
                      It will over-ride the study column value""")
  parser.add_argument('-N', '--batch-size', type=int,
                      help="""Size of the batch of individuals
                      to be processed in parallel (if possible)""",
                      default=1000)

def import_individual_implementation(args):
  recorder = Recorder(args.study, args.host, args.user, args.passwd,
                      args.keep_tokens, args.batch_size)
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
  print 'ready to go on file %s' % args.ifile.name
  import_pedigree(recorder, istream(f, recorder.input_rows))
  recorder.clean_up()


def do_register(registration_list):
  registration_list.append(('individual', help_doc,
                            make_parser_individual,
                            import_individual_implementation))

