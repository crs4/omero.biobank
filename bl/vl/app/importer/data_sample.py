"""
Import of Data samples
======================

Will read in a tsv file with the following columns::

  study label sample_label device_name device_maker device_model device_release [optional columns]


  ....

Record that point to an unknown (sample_label) will be noisily
ignored. The same will happen to records that have the same label of a
previously seen data sample.

The general strategy is to decide what data objects should be
instantiated by looking at the device_{maker,model,release} columns.

Currently supported triples and corresponding vl object::

  maker      model            release    object         opt_columns
  Affymetrix GenomeWideSNP_6  1.0        AffymetrixCel  contrast_qc


  study   label       contrast_qc   sample_label        device_maker  device_model    device_release  device_name
  TEST01  a520532.CEL 2.73  SMP4_0005515:[0,1]  Affymetrix    GenomeWideSNP_6 1.0             Affymetrix-inc-X

"""


from bl.vl.sample.kb import KBError
from core import Core, BadRecord
from version import version

import csv, json
import time, sys

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

class Recorder(Core):
  """
  An utility class that handles the actual recording of DataSample(s)
  metadata into VL, including the potential actual saving of datasets.
  """
  def __init__(self, study_label=None, data_dir=None, load_data_objects=False,
               host=None, user=None, passwd=None, keep_tokens=1, operator='Alfred E. Neumann'):
    """
    FIXME

    :param data_dir:
    :type data_dir:
    """
    super(Recorder, self).__init__(host, user, passwd)
    self.data_dir = data_dir
    self.load_data_objects = load_data_objects
    #FIXME this can probably go to core....
    self.default_study = None
    if study_label:
      s = self.skb.get_study_by_label(study_label)
      if not s:
        raise ValueError('No known study with label %s' % study_label)
      self.logger.info('Selecting %s[%d,%s] as default study' % (s.label, s.omero_id, s.id))
      self.default_study = s
    #-------------------------
    self.known_studies = {}
    self.device = self.get_device('importer-0.0', 'CRS4', 'IMPORT', '0.0')
    self.asetup = self.get_action_setup('importer-version-%s-%s-%f' % (version, "DataSample", time.time()),
                                        # FIXME the json below should
                                        # record the app version, and the
                                        # parameters used.  unclear if we
                                        # need to register the file we load
                                        # data from, since it is, most
                                        # likely, a transient object.
                                        json.dumps({'study' : study_label,
                                                    'data_dir' : self.data_dir,
                                                    'load_data_objects' : self.load_data_objects,
                                                    'operator' : operator,
                                                    'host' : host,
                                                    'user' : user}))
    self.acat  = self.acat_map['IMPORT']
    self.operator = operator
    #
    self.input_rows = {}
    self.counter = 0

  @debug_wrapper
  def get_study_by_label(self, study_label):
    if self.default_study:
      return self.default_study
    return self.known_studies.setdefault(study_label,
                                         super(Recorder, self).get_study_by_label(study_label))

  @debug_wrapper
  def get_action(self, sample, name, maker, model, release):
    device = self.get_device(name, maker, model, release)
    asetup = self.get_action_setup('import-%s-%s' % (maker, model),
                                   json.dumps({}))
    if sample.__class__.name == 'PlateWell':
      action = self.create_action_on_sample_slot(sample, device, asetup)
    elif sample.__class__.name == 'DNASample':
      action = self.create_action_on_sample(sample, device, asetup)
    else:
      raise ValueError('sample [%s] is of the wrong type' % sample.label)
    return action

  @debug_wrapper
  def record(self, r):
    self.logger.debug('\tworking on %s' % r)
    try:
      study = self.get_study_by_label(r['study'])
      #-
      sample = self.get_bio_sample(label=sample_label)
      if not sample:
        raise ValueError('could not find a sample with label %s' % sample_label)
      #-
      name, maker, model, release = r['device_name'], r['device_maker'], r['device_model'], r['device_release']
      action = self.get_action(sample, name, maker, model, release)
      #-
      if maker == 'Affymetrix' and model  == 'GenomeWideSNP_6':
        data_sample = self.skb.AffymetrixCel(name=r['label'],
                                             array_type='GenomeWideSNP_6',
                                             data_type=self.dtype_map['GTRAW']) # FIXME this is stupid
        data_sample.action  = action
        data_sample.outcome = self.outcome_map['OK']
        if r.has_key('contrast_qc'):
          data_sample.contrastQC = r['contrast_qc']
        data_sample = self.skb.save(data_sample)
      else:
        raise ValueError('%s, %s not supported' % (maker, model))

    except KeyError, e:
      self.logger.warn('ignoring record %s because of missing value(%s)' % (r, e))
      return
    # except ValueError, e:
    #   logger.warn('ignoring record %s since %s' % (r, e))
    #   return
    # except (KBError, NotImplementedError), e:
    #   logger.warn('ignoring record %s because it triggers a KB error: %s' % (r, e))
    #   return
    # except Exception, e:
    #   logger.fatal('INTERNAL ERROR WHILE PROCESSING %s (%s)' % (r, e))
    #   sys.exit(1)

  @debug_wrapper
  def create_action_on_sample(self, study, sample, device, asetup, description):
    return self.create_action_helper(self.skb.ActionOnSample, description,
                                     study, device, asetup,
                                     self.acat, self.operator, sample)

  @debug_wrapper
  def create_action_on_sample_slot(self, study, sample_slot, device, asetup, description):
    return self.create_action_helper(self.skb.ActionOnSamplesContainerSlot, description,
                                     study, device, asetup, self.acat, self.operator,
                                     sample_slot)

  @debug_wrapper
  def get_bio_sample(self, label):
    bio_sample = self.skb.get_bio_sample(label=label)
    if not bio_sample:
      raise ValueError('cannot find a sample with label <%s>' % label)
    return  bio_sample

help_doc = """
import new data_sample definitions into a virgil system. It will also
import actual datasets if so instructed.
"""

def make_parser_data_sample(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default conxtest study label.
                      It will over-ride the study column value""")
  parser.add_argument('-d', '--data-dir', type=str,
                      help="""directory that contains data object files""")
  parser.add_argument('--load-data-objects', action='store_true', default=False,
                      help="""if set, load datasets in VL""")

def import_data_sample_implementation(args):
  recorder = Recorder(args.study, plate_shape=plate_shape,
                      volume=args.volume, update_volume=args.update_volume,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  f = csv.DictReader(args.ifile, delimiter='\t')
  for r in f:
    recorder.record(r)

def do_register(registration_list):
  registration_list.append(('data_sample', help_doc,
                            make_parser_data_sample,
                            import_data_sample_implementation))


