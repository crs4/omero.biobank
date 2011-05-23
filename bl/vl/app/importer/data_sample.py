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
  Affymetrix GenomeWideSNP_6  1.0        AffymetrixCel  contrastQC


  study   label       contrastQC   sample_label        device_maker  device_model    device_release  device_name
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
  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               operator='Alfred E. Neumann', message=None):
    """
    FIXME

    :param data_dir:
    :type data_dir:
    """
    super(Recorder, self).__init__(host, user, passwd)
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
                                                    'operator' : operator,
                                                    'host' : host,
                                                    'user' : user,
                                                    'message' : message}))
    self.acat  = self.acat_map['IMPORT']
    self.operator = operator
    #
    self.input_rows = {}
    self.counter = 0
    #--------------------------------------------------------------------------
    self.logger.info('start prefetching DNASample(s)')
    dna_samples = self.skb.get_bio_samples(self.skb.DNASample)
    self.dna_samples = {}
    for ds in dna_samples:
      self.dna_samples[ds.label] = ds
    self.logger.info('done prefetching DNASample(s)')
    self.logger.info('there are %d DNASample(s) in the kb' %
                     len(self.dna_samples))
    #-
    self.logger.info('start prefetching TiterPlate(s)')
    # FIXME this method has a funny name
    self.titer_plates = {}
    self.titer_plates_by_omero_id = {}
    tps = self.skb.get_bio_samples(self.skb.TiterPlate)
    for tp in tps:
      self.titer_plates[tp.label] = tp
      self.titer_plates_by_omero_id[tp.omero_id] = tp
    self.logger.info('done prefetching TiterPlate(s)')
    self.logger.info('there are %d TiterPlate(s) in the kb' %
                     len(self.titer_plates))
    #-
    self.plate_wells = {}
    #FIXME this is deeply wrong. PlateWell.label is optional and NOT
    #unique. It will work now because we are generating them. It should be fixed.
    self.plate_wells_by_label = {}
    self.logger.info('start prefetching PlateWell(s)')
    # FIXME this method has a funny name
    pws = self.skb.get_bio_samples(self.skb.PlateWell)
    for pw in pws:
      k = (self.titer_plates_by_omero_id[pw.container.omero_id],
           pw.slotPosition)
      self.plate_wells[k] = pw
      self.plate_wells_by_label[pw.label] = pw
    self.logger.info('done prefetching PlateWell(s)')
    self.logger.info('there are %d PlateWell(s) in the kb' % len(self.plate_wells))
    #--
    data_samples = self.skb.get_bio_samples(self.skb.DataSample)
    self.data_samples = {}
    for ds in data_samples:
      self.data_samples[ds.label] = ds
    self.logger.info('done prefetching DataSample(s)')
    self.logger.info('there are %d DataSample(s) in the kb' % len(self.data_samples))

  @debug_wrapper
  def get_study_by_label(self, study_label):
    if self.default_study:
      return self.default_study
    return self.known_studies.setdefault(study_label,
                                         super(Recorder, self).get_study_by_label(study_label))

  @debug_wrapper
  def get_action(self, study, sample, name, maker, model, release):
    device = self.get_device(name, maker, model, release)
    asetup = self.get_action_setup('import-%s-%s' % (maker, model),
                                   json.dumps({}))
    if sample.__class__.__name__ == 'PlateWell':
      action = self.create_action_on_sample_slot(study, sample, device, asetup,
                                                 description='')
    elif sample.__class__.__name__ == 'DNASample':
      action = self.create_action_on_sample(study, sample, device, asetup,
                                            description='')
    else:
      raise ValueError('sample [%s] is of the wrong type' % sample.label)
    return action

  @debug_wrapper
  def record(self, r):
    self.logger.debug('\tworking on %s' % r)
    try:
      name, maker, model, release = r['device_name'], r['device_maker'], \
                                    r['device_model'], r['device_release']
      label, sample_label = r['label'], r['sample_label']
      #-
      if self.data_samples.has_key(label):
        self.logger.warn('DataSample with same name %s in kb, ignoring this record' %
                         label)
        return

      study = self.get_study_by_label(r['study'])
      #-
      if maker == 'Affymetrix' and model == 'GenomeWideSNP_6':
        if self.plate_wells_by_label.has_key(sample_label):
          sample = self.plate_wells_by_label[sample_label]
          self.logger.info('using sample %s[%s]' % (sample.__class__.__name__, sample.label))
        else:
          sample = self.get_bio_sample(self.skb.PlateWell, label=sample_label)
      #-
      if not sample:
        raise ValueError('could not find a sample with label %s' % sample_label)
      #-
      action = self.get_action(study, sample, name, maker, model, release)
      #-
      if maker == 'Affymetrix' and model  == 'GenomeWideSNP_6':
        data_sample = self.skb.AffymetrixCel(label=label,
                                             array_type='GenomeWideSNP_6',
                                             data_type=self.dtype_map['GTRAW']) # FIXME this is stupid
        data_sample.action  = action
        data_sample.outcome = self.outcome_map['OK']
        if r.has_key('contrastQC'):
          data_sample.contrastQC = float(r['contrastQC'])
        data_sample = self.skb.save(data_sample)
        self.logger.info('saved data_sample %s[%s]' % (data_sample.__class__.__name__,
                                                       data_sample.label))
      else:
        raise ValueError('%s, %s not supported' % (maker, model))

    except KeyError, e:
      self.logger.warn('ignoring record %s because of missing value(%s)' % (r, e))
      return
    except ValueError, e:
      self.logger.warn('ignoring record %s since %s' % (r, e))
      return
    except (KBError, NotImplementedError), e:
      self.logger.warn('ignoring record %s because it triggers a KB error: %s' % (r, e))
      return
    except Exception, e:
      self.logger.fatal('INTERNAL ERROR WHILE PROCESSING %s (%s)' % (r, e))
      return

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
  def get_bio_sample(self, aklass, label):
    bio_sample = self.skb.get_bio_sample(aklass, label=label)
    if not bio_sample:
      raise ValueError('cannot find a sample with label <%s>' % label)
    return  bio_sample

help_doc = """
import new data_sample definitions into a virgil system.
"""

def make_parser_data_sample(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default conxtest study label.
                      It will over-ride the study column value""")
  parser.add_argument('--default-contrast-qc', type=float,
                      help="""if contrastQC is not defined, assign this value""")

def import_data_sample_implementation(args):
  # FIXME we should find a clean way to record command-line options
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens,
                      message='default-contrast-qc=%s' % args.default_contrast_qc)
  f = csv.DictReader(args.ifile, delimiter='\t')
  for r in f:
    if (args.default_contrast_qc
        and  (not r.has_key('contrastQC')
              or r['contrastQC'] == 'None')):
      r['contrastQC']  = args.default_contrast_qc
    recorder.record(r)

def do_register(registration_list):
  registration_list.append(('data_sample', help_doc,
                            make_parser_data_sample,
                            import_data_sample_implementation))


