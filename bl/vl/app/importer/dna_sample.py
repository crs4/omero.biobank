"""
Import of DNA samples
=====================

Will read in a csv file with the following columns::

  study label  barcode blood_sample_label initial_volume current_volume status nanodrop qp230260 qp230280
  xxx   dn01   2903902 bs-label 9.5 6.5 40 0.4 0.5 USABLE
  ....

Volume units are FIXME ml
Records that point to an unknown blood_sample_label will be noisily
ignored. The same will happen to records that have the same label or
barcode of a previously seen dna sample.

Study defines the context in which the import occurred, the blood
sample is identified by its label, which is enforced to be unique
(as far as BloodSample samples are concerned) in VL. In the same way,
the imported sample will be uniquely identified by its barcode. The
sample label will be set to the string <study>-<label>, which will be
enforced to be unique too within VL.

"""

from bl.vl.sample.kb import KBError
from bio_sample import BioSampleRecorder, BadRecord

import csv, json, sys

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

class Recorder(BioSampleRecorder):
  """
  An utility class that handles the actual recording of DNASample(s) into VL
  """
  def __init__(self, study_label=None, initial_volume=None, current_volume=None,
               host=None, user=None, passwd=None, keep_tokens=1, operator='Alfred E. Neumann'):
    super(Recorder, self).__init__('DNASample',
                                   study_label, initial_volume, current_volume,
                                   host, user, passwd, keep_tokens, operator)

  @debug_wrapper
  def create_action(self, study, blood_sample, description=''):
    return self.create_action_helper(self.skb.ActionOnSample, description,
                                     study, self.device,
                                     self.asetup, self.acat, self.operator,
                                     blood_sample)

  @debug_wrapper
  def create_dna_sample(self, study, blood_sample, label, barcode,
                        initial_volume, current_volume, status,
                        nanodrop, qp230260, qp230280):
    assert label and barcode and initial_volume >= current_volume
    action = self.create_action(study, blood_sample, description=json.dumps(self.input_rows[barcode]))
    #--
    sample = self.skb.DNASample()
    sample.action, sample.outcome   = action, self.outcome_map['OK']
    sample.labLabel = label
    sample.barcode  = barcode
    sample.initialVolume = initial_volume
    sample.currentVolume = current_volume
    sample.status = self.sstatus_map[status.upper()]
    sample.nanodropConcentration = nanodrop
    sample.qp230260 = qp230260
    sample.qp230280 = qp230280

    logger.debug('\tsaving dna_sample(>%s<,>%s<)' % (sample.labLabel,
                                                     sample.barcode))
    return self.skb.save(sample)


  @debug_wrapper
  def record(self, r):
    logger.debug('\tworking on %s' % r)
    klass = self.skb.DNASample
    try:
      study, label, barcode, initial_volume, current_volume, status = \
             self.record_helper(klass.__name__, r)
      blood_sample_label = r['blood_sample_label']
      nanodrop, qp230260, qp230280 = [r[k] for k in 'nanodrop qp230260 qp230280'.split()]
      nanodrop = int(nanodrop)
      qp230260 = float(qp230260)
      qp230280 = float(qp230280)

      blood_sample = self.skb.get_blood_sample(label=blood_sample_label)
      if not blood_sample:
        logger.warn('ignoring record %s because there is not a blood_sample with that label' % r)
        return

      self.create_dna_sample(study, blood_sample, label, barcode,
                             initial_volume, current_volume, status,
                             nanodrop, qp230260, qp230280)
    except BadRecord, msg:
      logger.warn('ignoring record %s: %s' % (r, msg))
    except KeyError, e:
      logger.warn('ignoring record %s because of missing value(%s)' % (r, e))
      return
    except ValueError, e:
      logger.warn('ignoring record %s because of conversion errors(%s)' % (r, e))
      return
    except (KBError, NotImplementedError), e:
      logger.warn('ignoring record %s because it triggers a KB error: %s' % (r, e))
      return
    except Error, e:
      logger.fatal('INTERNAL ERROR WHILE PROCESSING %s (%s)' % (r, e))
      sys.exit(1)

help_doc = """
import new dna sample definitions into a virgil system and attach
them to previously registered blood samples.
"""

def make_parser_dna_sample(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default study assumed for the reference individuals.
                      It will over-ride the study column value""")
  parser.add_argument('-V', '--initial-volume', type=float,
                      help="""default initial volume assigned to the blood sample.
                      It will over-ride the initial_volume column value""")
  parser.add_argument('-C', '--current-volume', type=float,
                      help="""default current volume assigned to the blood sample.
                      It will over-ride the initial_volume column value.""")


def import_dna_sample_implementation(args):
  recorder = Recorder(args.study,
                      initial_volume=args.initial_volume, current_volume=args.current_volume,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  f = csv.DictReader(args.ifile, delimiter='\t')
  for r in f:
    recorder.record(r)

def do_register(registration_list):
  registration_list.append(('dna_sample', help_doc,
                            make_parser_dna_sample,
                            import_dna_sample_implementation))


