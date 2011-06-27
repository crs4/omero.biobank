"""
Import of Data samples
======================

Will read in a tsv file with the following columns::

  study  label sample_label device_label options
  ASTUDY foo01 pl05:A03     chip90       celID=0009099090
  ....

where options is a comma separated list of key=value pairs.

The general strategy is to decide what data objects should be
instantiated by looking at the device_label column and to its
corresponding maker,model,release.

FIXME provide a way to dump the supported data object types.

"""

from core import Core, BadRecord
import csv, json
import itertools as it

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

def conf_affymetrix_cel_6(kb, r, a, options):
  conf = {'label' : r['label'],
          'status' : kb.DataSampleStatus.USABLE,
          'action' : a,
          'arrayType' : kb.AffymetrixCelArrayType.GenomeWideSNP_6,
          }
  if 'celID' in options:
    conf['celID'] = options['celID']
  return kb.factory.create(kb.AffymetrixCel, conf)

data_sample_configurator = {
  ('Affymetrix', 'Genome-Wide Human SNP Array', '6.0') : conf_affymetrix_cel_6,
  }

class Recorder(Core):
  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann'):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   study_label)
    self.batch_size = batch_size
    self.operator = operator
    self.known_barcodes = []
    self.known_devices = {}
    self.known_tubes = {}
    self.known_data_samples = {}

  def record(self, records):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    #--
    if not records:
      self.logger.warn('no records')
      return
    self.choose_relevant_study(records)
    self.preload_devices()
    self.preload_data_samples()
    self.preload_tubes()
    #--
    records = self.do_consistency_checks(records)
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(c)
      self.logger.info('done processing chunk %d' % i)

  def choose_relevant_study(self, records):
    if self.default_study:
      return
    study_label = records[0]['study']
    for r in records:
      if r['study'] != study_label:
        m = 'all records should have the same study label'
        self.logger.critical(m)
        raise ValueError(m)
    self.default_study = self.get_study(study_label)

  def build_well_full_label(self, clabel, wlabel):
    return ':'.join([clabel, wlabel])

  def preload_labelled_vessels(self, klass, content, preloaded):
    vessels = self.kb.get_vessels(klass, content)
    for v in vessels:
      if hasattr(v, 'label'):
        if isinstance(v, self.kb.PlateWell):
          #FIXME should we check that v.container is an actual TiterPlate?
          plate = v.container
          plate.reload() # Do we really need to do this? We have
                         # already preloaded all Plates...
          label = self.build_well_full_label(plate.label, v.label)
        else:
          label = v.label
        preloaded[label] = v
      if hasattr(v, 'barcode') and v.barcode is not None:
        self.known_barcodes.append(v.barcode)

  def preload_tubes(self):
    self.logger.info('start prefetching vessels')
    self.preload_labelled_vessels(klass=self.kb.Vessel,
                                  content=None,
                                  preloaded=self.known_tubes)
    self.logger.info('there are %d labelled Vessel(s) in the kb'
                     % (len(self.known_tubes)))

  def preload_devices(self):
    self.logger.info('start prefetching devices')
    devices = self.kb.get_objects(self.kb.Device)
    for d in devices:
      self.known_devices[d.label] = d
      if hasattr(d, 'barcode') and d.barcode is not None:
        self.known_barcodes.append(d.barcode)
    self.logger.info('there are %d Device(s) in the kb'
                     % (len(self.known_devices)))

  def preload_data_samples(self):
    self.logger.info('start prefetching data samples')
    ds = self.kb.get_objects(self.kb.DataSample)
    for d in ds:
      self.known_data_samples[d.label] = d
    self.logger.info('there are %d DataSample(s) in the kb'
                     % (len(self.known_data_samples)))

  #----------------------------------------------------------------
  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    #--
    k_map = {}
    good_records = []
    for i, r in enumerate(records):
      reject = ' Rejecting import of row %d.' % i
      if r['label'] in self.known_data_samples:
        f = 'there is a pre-existing DataSample with label %s.' + reject
        self.logger.warn(f % r['label'])
        continue
      if r['label'] in k_map:
        f = ('there is a pre-existing DataSample with label %s.(in this batch)'
             + reject)
        self.logger.error(f % r['label'])
        continue
      if (not r['sample_label'] in self.known_tubes and
          not r['sample_label'] in self.known_data_samples):
        f = 'there is no known sample with label %s.' + reject
        self.logger.error(f % r['sample_label'])
        continue
      if not r['device_label'] in self.known_devices:
        f = 'there is no known device with label %s.' + reject
        self.logger.error(f % r['device_label'])
        continue
      dev = self.known_devices[r['device_label']]
      k = (dev.maker, dev.model, dev.release)
      if not k in data_sample_configurator:
        f = 'there is no known mapping for device_label %s.' + reject
        self.logger.error(f % r['device_label'])
        continue
      if 'options' in r and r['options'] is not 'None':
        try:
          kvs = r['options'].split(',')
          for kv in kvs:
            k,v = kv.split('=')
        except ValueError, e:
          f = 'illegal options string.' + reject
          self.logger.error(f)
          continue
      k_map[r['label']] = r
      good_records.append(r)
    self.logger.info('done consistency checks')
    #--
    return good_records

  def process_chunk(self, chunk):
    def get_options(r):
      options = {}
      if 'options' in r and r['options'] is not 'None':
        kvs = r['options'].split(',')
        for kv in kvs:
          k, v = kv.split('=')
          options[k] = v
      return options
    #--
    actions = []
    for r in chunk:
      sample_label = r['sample_label']
      #--
      if sample_label in self.known_tubes:
        target = self.known_tubes[sample_label]
        a_klass = self.kb.ActionOnVessel
        acat = self.kb.ActionCategory.MEASUREMENT
      elif sample_label in self.known_data_samples:
        target = self.known_data_samples[sample_label]
        a_klass = self.kb.ActionOnDataSample
        acat = self.kb.ActionCategory.PROCESSING
      #--
      options = get_options(r)
      alabel = ('importer.data_sample.%s' % r['label'])
      asetup = self.kb.factory.create(self.kb.ActionSetup,
                                      {'label' : alabel,
                                       'conf' : json.dumps(options)})
      device = self.known_devices[r['device_label']]
      conf = {'setup' : asetup,
              'device': device,
              'actionCategory' : acat,
              'operator' : self.operator,
              'context'  : self.default_study,
              'target' : target
              }
      actions.append(self.kb.factory.create(a_klass, conf))
    self.kb.save_array(actions)
    #--
    data_samples = []
    for a,r in it.izip(actions, chunk):
      # FIXME we need to do this, otherwise the next save will choke.
      a.unload()
      dev = self.known_devices[r['device_label']]
      k = (dev.maker, dev.model, dev.release)
      d = data_sample_configurator[k](self.kb, r, a, get_options(r))
      data_samples.append(d)
    #--
    self.kb.save_array(data_samples)
    #--
    for d in data_samples:
      self.logger.info('saved %s as %s.' % (d.label, d.id))

def make_parser_data_sample(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default study used as context
                      for the import action.  It will
                      over-ride the study column value.""")
  parser.add_argument('-N', '--batch-size', type=int,
                      help="""Size of the batch of individuals
                      to be processed in parallel (if possible)""",
                      default=1000)

def import_data_sample_implementation(args):
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  #--
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  recorder.record(records)
  logger.info('done processing file %s' % args.ifile.name)
  #--

help_doc = """
import new data sample definitions into a virgil system and attach
them to previously registered samples.
"""

def do_register(registration_list):
  registration_list.append(('data_sample', help_doc,
                            make_parser_data_sample,
                            import_data_sample_implementation))


