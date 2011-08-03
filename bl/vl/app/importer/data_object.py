"""
Import of Data Objects
======================

Will read in a tsv file with the following columns::

   study path data_sample mimetype size sha1

   TEST01 file:/share/fs/v039303.cel V2902 x-vl/affymetrix-cel 39090 E909090
  ....

Records that point to an unknown (data_sample) will be noisily
ignored. The same will happen to records that have the same path of a
previously seen data_object
"""
from core import Core, BadRecord

import csv, json, time

import itertools as it

SUPPORTED_MIME_TYPES = ['x-vl/affymetrix-cel']

class Recorder(Core):
  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               action_setup_conf=None,
               batch_size=1000, operator='Alfred E. Neumann',
               logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   study_label, logger=logger)
    self.batch_size = batch_size
    self.operator = operator
    self.action_setup_conf = action_setup_conf
    self.preloaded_data_objects = {}
    self.preloaded_data_samples = {}

  def record(self, records):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size

    if not records:
      self.logger.warn('no records')
      return

    study = self.find_study(records)

    self.preload_data_samples()
    self.preload_data_objects()

    records = self.do_consistency_checks(records)

    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(c)
      self.logger.info('done processing chunk %d' % i)

  def preload_data_samples(self):
    self.preload_by_type('data_sample', self.kb.DataSample,
                         self.preloaded_data_samples)

  def preload_data_objects(self):
    self.logger.info('start preloading data objects')
    ds = self.kb.get_objects(self.kb.DataObject)
    for d in ds:
      self.preloaded_data_objects[d.path] = d
    self.logger.info('there are %d DataObject(s) in the kb'
                     % (len(self.preloaded_data_objects)))

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    #--
    good_records = []
    for i, r in enumerate(records):
      reject = ' Rejecting import of record %d.' % i

      try:
        for k in ['path', 'mimetype', 'size', 'sha1']:
          r[k]
      except KeyError, e:
        f = 'missing field %s.' + reject
        self.logger.error(f % k)
        continue

      if r['path'] in self.preloaded_data_objects:
        f = 'there is a pre-existing data_object with path %s. ' + reject
        self.logger.warn(f % r['path'])
        continue

      if not r['data_sample'] in self.preloaded_data_samples:
        f = 'there is no known data_sample with id %s.' + reject
        self.logger.error(f % r['data_sample'])
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
    return good_records

  def process_chunk(self, chunk):
    data_objects = []
    for r in chunk:
      sample = self.preloaded_data_samples[r['data_sample']]
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
                       (do.path, do.mimetype, do.size, do.sample.id))

def canonize_records(args, records):
  fields = ['study', 'mimetype']
  for f in fields:
    if hasattr(args, f) and getattr(args,f) is not None:
      for r in records:
        r[f] = getattr(args, f)

def make_parser_data_object(parser):
  parser.add_argument('--study', type=str,
                      help="""default study used as context
                      for the import action.  It will
                      over-ride the study column value, if any.""")
  parser.add_argument('--mimetype', type=str,
                      choices=SUPPORTED_MIME_TYPES,
                      help="""default mimetype.  It will
                      over-ride the mimetype column value, if any.""")

def import_data_object_implementation(logger, args):

  action_setup_conf = self.find_action_setup_conf(args)

  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      logger=logger)

  #--
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]

  canonize_records(args, records)

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
