"""
Import of blood samples
,,,,,,,,,,,,,,,,,,,,,,,


Will read in a csv file with the following columns::

  study label bslabel bsbarcode initial_volume current_volume status
  xxx   id2   bs01    328989238 20             20             USABLE
  xxx   id3   bs03    328989228 20             20             USABLE
  ....

Record that point to an unknown (study, label) pair will be noisily
ignored. The same will happen to records that have the same bslabel or
bsbarcode of a previously seen blood sample.
"""

from bl.vl.sample.kb     import KBError
from bl.vl.sample.kb     import KnowledgeBase as sKB
from bl.vl.individual.kb import KnowledgeBase as iKB

from core import Core

import csv, json

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

class Recorder(Core):
  """
  An utility class that handles the actual recording into VL
  """
  def __init__(self, study_label=None, initial_volume=None, current_volume=None,
               host=None, user=None, passwd=None):
    super(Recorder, self).__init__(host, user, passwd)
    self.default_study = None
    if study_label:
      s = self.skb.get_study_by_label(study_label)
      if not s:
        raise ValueError('No known study with label %s' % study_label)
      logger.info('Selecting %s[%d,%s] as default study' % (s.label, s.omero_id, s.id))
      self.default_study = s
    self.known_studies = {}
    self.device = self.get_device('CRS4', 'IMPORT', '0.0')
    self.asetup = self.get_action_setup('import-prog-%f' % time.time(),
                                        # FIXME the json below should
                                        # record the app version, and the
                                        # parameters used.  unclear if we
                                        # need to register the file we load
                                        # data from, since it is, most
                                        # likely, a transient object.
                                        json.dumps({'study' : study_label,
                                                    'initial_volume' : initial_volume,
                                                    'current_volume' : current_volume,
                                                    'host' : host,
                                                    'user' : user}))
    self.acat  = self.acat_map['IMPORT']
    # FIXME this should be detected from the env.
    self.operator = 'Alfred E. Neumann'
    #
    self.input_rows = {}
    self.counter = 0

  @debug_wrapper
  def create_blood_sample(self, enrollment, initial_volume, current_volume):
    action = self.create_action_on_individual(enrollment, device, asetup, acat, operator)
    #--
    sample = self.skb.BloodSample()
    sample.action, sample.outcome   = action, self.outcome_map['OK']
    sample.labLabel = '%s-%s' % (self.study_label, enrollment.studyCode)
    sample.barcode  = sample.id
    sample.initialVolume = initial_volume
    sample.currentVolume = current_volume
    sample.status = self.sstatus_map['USABLE']
    return sample


  @debug_wrapper
  def record(self, r):
    logger.debug('\tworking on %s' % r)
    study_code = r['label']
    e = self.ikb.get_enrollment(study, study_code=study_code)
    if not e:
      logger.warn('ignoring record %s because of unkown enrollment reference (%s,%s)' % \
                  (r, study.label, study_code))



def make_parser_blood_sample(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default study assumed for the reference individuals.
                      It will over-ride the study column value""")
  parser.add_argument('-V', '--initial-volume', type=float,
                      help="""default initial volume assigned to the blood sample.
                      It will over-ride the initial_volume column value""")
  parser.add_argument('-C', '--current-volume', type=float,
                      help="""default current volume assigned to the blood sample.
                      It will over-ride the initial_volume column value.""")

def import_blood_sample_implementation(args):
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
  for r in f:
    import_blood_sample(r



help_doc = """
import new blood sample definitions into a virgil system and attach
them to previously registered patients.
"""

def make_parser_individual(parser):
  pass

def import_blood_sample_implementation(args):
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
  registration_list.append(('blood_sample', help_doc,
                            make_parser_blood_sample,
                            import_blood_sample_implementation))


