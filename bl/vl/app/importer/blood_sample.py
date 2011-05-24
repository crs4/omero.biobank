"""
Import of blood samples
=======================


Will read in a csv file with the following columns::

  study label barcode      individual_label  initial_volume current_volume status
  xxx   bs01  328989238    id2               20             20             USABLE
  xxx   bs03  328989228    id3               20             20             USABLE
  ....

Records that point to an unknown (individual_study, individual_label) pair will be noisily
ignored. The same will happen to records that have the same label or
barcode of a previously seen blood sample.

Study defines the context in which the import occurred. The imported
sample will be uniquely identified in VL, within BloodSample by its
barcode. The sample label will be set to the string <study>-<label>,
which will be enforced to be unique within VL.

"""

from bio_sample import BioSampleRecorder, BadRecord

import csv, json


#-----------------------------------------------------------------------------
#FIXME this should be factored out....

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

class Recorder(BioSampleRecorder):
  """
  An utility class that handles the actual recording of BloodSample(s) into VL
  """
  def __init__(self, study_label=None, initial_volume=None, current_volume=None,
               host=None, user=None, passwd=None, keep_tokens=1, operator='Alfred E. Neumann'):
    super(Recorder, self).__init__('BloodSample',
                                   study_label, initial_volume, current_volume,
                                   host, user, passwd, keep_tokens, operator)
    #--
    self.known_enrollments = {}
    if self.default_study:
      self.logger.info('start pre-loading known enrolled individuala')
      known_enrollments = self.ikb.get_enrolled(self.default_study)
      for e in known_enrollments:
        self.known_enrollments[e.studyCode] = e
      self.logger.info('done pre-loading known enrolled individuals')
      self.logger.info('there are %d enrolled individuals in study %s' % (len(self.known_enrollments),
                                                                          self.default_study.label))
    #--
    self.logger.info('start prefetching BloodSample(s)')
    blood_samples = self.skb.get_bio_samples(self.skb.BloodSample)
    self.known_blood_samples = {}
    for bs in blood_samples:
      self.known_blood_samples[bs.label] = bs
    self.logger.info('done prefetching BloodSample(s)')
    self.logger.info('there are %d BloodSample(s) in the kb'
                     % (len(self.known_blood_samples)))
    #--


  @debug_wrapper
  def create_action(self, enrollment, description=''):
    return self.create_action_helper(self.ikb.ActionOnIndividual, description,
                                     enrollment.study, self.device,
                                     self.asetup, self.acat, self.operator,
                                     enrollment.individual)

  @debug_wrapper
  def create_blood_sample(self, enrollment, label, barcode,
                          initial_volume, current_volume, status):
    assert label and barcode and initial_volume >= current_volume
    action = self.create_action(enrollment,
                                description=json.dumps(self.input_rows[barcode]))
    #--
    #-- FIXME why are we not passing barcode too?
    sample = self.skb.BloodSample(label=label)
    sample.action, sample.outcome   = action, self.outcome_map['OK']
    sample.barcode  = barcode
    sample.initialVolume = initial_volume
    sample.currentVolume = current_volume
    sample.status = self.sstatus_map[status.upper()]
    sample = self.skb.save(sample)
    return sample

  @debug_wrapper
  def record(self, r):
    self.logger.info('processing record[%d] (%s,%s),'
                     % (self.record_counter, r['study'], r['label']))
    self.record_counter += 1
    if self.known_blood_samples.has_key(r['label']):
      self.logger.info('BloodSample %s is already in the kb' % r['label'])
      return
    #--
    klass = self.skb.BloodSample
    try:
      study, label, barcode, initial_volume, current_volume, status = \
             self.record_helper(klass.__name__, r)
      i_label = r['individual_label']
      if self.default_study and self.known_enrollments.has_key(i_label):
        e = self.known_enrollments[i_label]
      else:
        e = self.ikb.get_enrollment(study_label=study.label, ind_label=i_label)
      if not e:
        self.logger.warn('ignoring record %s because of unkown enrollment reference (%s,%s)' % \
                         (r, study.label, i_label))
        return

      sample = self.create_blood_sample(e, label, barcode,
                                        initial_volume, current_volume, status)
      self.logger.info('saving record (%s, %s)' %  (study.label, sample.label))

    except BadRecord, msg:
      self.logger.warn('ignoring record %s: %s' % (r, msg))
      return
    except KeyError, e:
      self.logger.warn('ignoring record %s because of missing value(%s)' % (r, e))
      return
    except Exception, e:
      self.logger.warn('ignoring record %s because of (%s)' % (r, e))
      return


def make_parser_blood_sample(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default study assumed for the reference individuals and
                      as context for the import action.  It will over-ride the study
                      column value.""")
  parser.add_argument('-V', '--initial-volume', type=float,
                      help="""default initial volume assigned to the blood sample.
                      It will over-ride the initial_volume column value.""")
  parser.add_argument('-C', '--current-volume', type=float,
                      help="""default current volume assigned to the blood sample.
                      It will over-ride the current_volume column value.""")

def import_blood_sample_implementation(args):
  recorder = Recorder(args.study,
                      initial_volume=args.initial_volume, current_volume=args.current_volume,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  logger.info('start processing file %s' % args.ifile.name)
  f = csv.DictReader(args.ifile, delimiter='\t')
  for r in f:
    recorder.record(r)
  logger.info('done processing file %s' % args.ifile.name)

help_doc = """
import new blood sample definitions into a virgil system and attach
them to previously registered patients.
"""

def do_register(registration_list):
  registration_list.append(('blood_sample', help_doc,
                            make_parser_blood_sample,
                            import_blood_sample_implementation))


