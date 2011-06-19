"""
Import of Data samples
======================

Will read in a tsv file with the following columns::

   study path data_sample_label mimetype size sha1

   TEST01 file:/share/fs/v039303.cel CA_03030.CEL x-vl/affymetrix-cel 39090 E909090
  ....

Record that point to an unknown (data_sample_label) will be noisily
ignored. The same will happen to records that have the same path of a
previously seen data_object

"""
from core import Core, BadRecord

import csv, json

import itertools as it

SUPPORTED_MIME_TYPES = ['x-vl/affymetrix-cel']

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
  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann'):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   study_label)
    self.batch_size = batch_size
    self.operator = operator
    self.device = self.get_device(label='importer.data_object',
                                  maker='CRS4', model='importer', release='0.1')
    self.asetup = self.get_action_setup('importer.data_object',
                                        {'study_label' : study_label,
                                         'operator' : operator})
    self.known_data_objects = {}
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
    self.preload_data_samples()
    self.preload_data_objects()
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

  def preload_data_objects(self):
    self.logger.info('start prefetching data objects')
    ds = self.kb.get_objects(self.kb.DataObject)
    for d in ds:
      self.known_data_objects[d.path] = d
    self.logger.info('there are %d DataObject(s) in the kb'
                     % (len(self.known_data_objects)))

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
    good_records = []
    for i, r in enumerate(records):
      reject = ' Rejecting import of record %d.' % i
      if r['path'] in self.known_data_objects:
        f = 'there is a pre-existing data_object with path %s. ' + reject
        self.logger.warn(f % r['path'])
        continue
      if not r['data_sample_label'] in self.known_data_samples:
        f = 'there is no known data_sample with label %s. ' + reject
        self.logger.error(f % r['data_sample_label'])
        continue
      k = 'mimetype'
      if not k in r:
        f = 'missing field %s.' + reject
        self.logger.error(f % k)
        continue
      k = 'size'
      if not k in r:
        f = 'missing field %s.' + reject
        self.logger.error(f % k)
        continue
      k = 'sha1'
      if not k in r:
        f = 'missing field %s.' + reject
        self.logger.error(f % k)
        continue
      if r['mimetype'] not in SUPPORTED_MIME_TYPES:
        f = 'unknown mimetype %s.' + reject
        self.logger.error(f % r['mimetype'])
        continue
      if not r['size'].isdigit():
        f = 'bad size value %s.' + reject
        self.logger.error(f % r['size'])
        continue
      good_records.append(r)
    self.logger.info('done consistency checks')
    #--
    return good_records

  def process_chunk(self, chunk):
    data_objects = []
    for r in chunk:
      sample = self.known_data_samples[r['data_sample_label']]
      conf = {'path' : r['path'],
              'mimetype' : r['mimetype'],
              'size' : int(r['size']),
              'sample' : sample,
              'sha1' : r['sha1']}
      data_objects.append(self.kb.factory.create(self.kb.DataObject, conf))
    #--
    self.kb.save_array(data_objects)
    for do in data_objects:
      self.logger.info('saved %s[%s,%s] as attached to %s' %
                       (do.path, do.mimetype, do.size, do.sample.label))

def make_parser_data_object(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default study used as context
                      for the import action.  It will
                      over-ride the study column value.""")

def import_data_object_implementation(args):
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
import new data object definitions into a virgil system and attach
them to previously registered data samples.
"""

def do_register(registration_list):
  registration_list.append(('data_object', help_doc,
                            make_parser_data_object,
                            import_data_object_implementation))

