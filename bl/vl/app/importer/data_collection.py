"""
Import of Data Collection
=========================

Will read in a tsv file with the following columns::

  study    label data_sample
  BSTUDY   dc-01 V0390290
  BSTUDY   dc-01 V0390291
  BSTUDY   dc-02 V0390292
  BSTUDY   dc-02 V390293
  ....

This will create new DataCollection(s), whose label is defined by the
label column, and link to it, using DataCollectionItem objects,
the DataSample object identified by data_sample (a vid).

Record that point to an unknown (data_sample) will abort the
data collection loading. Previously seen collections will be noisily
ignored too. No, it is not legal to use the importer to add items to a
previously known collection.
"""

from core import Core, BadRecord

import csv, json, time

import itertools as it

class Recorder(Core):
  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann',
               logger=None, action_setup_conf=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   study_label, logger=logger)
    self.batch_size = batch_size
    self.operator = operator
    self.action_setup_conf = action_setup_conf
    self.preloaded_data_samples = {}
    self.preloaded_data_collections = {}

  def record(self, records, otsv):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size

    if len(records) == 0:
      self.logger.warn('no records')
      return

    study = self.find_study(records)
    self.data_sample_klass = self.find_data_sample_klass(records)
    self.preload_data_samples()
    self.preload_data_collections()

    records = self.do_consistency_checks(records)
    if len(records) == 0:
      self.logger.warn('no records')
      return

    asetup = self.get_action_setup('importer.data_collection-%f' % time.time(),
                                   json.dumps(self.action_setup_conf))
    device = self.get_device('importer-%s.data_collection' % version,
                             'CRS4', 'IMPORT', version)
    conf = {'setup' : asetup,
            'device': device,
            'actionCategory' : self.kb.ActionCategory.PROCESSING,
            'operator' : self.operator,
            'context'  : study
            }
    action = self.kb.factory.create(self.kb.Action, conf).save()

    def keyfunc(r): return r['label']
    records = sorted(records, key=keyfunc)

    for k, g in it.groupby(data, keyfunc):
      dc_conf = {'label' : k, 'action' : action}
      dc = self.kb.factory.create(self.kb.DataCollection, dc_conf).save()
      for i, c in enumerate(records_by_chunk(self.batch_size, g)):
        self.logger.info('start processing chunk %s-%d' % (k, i))
        self.process_chunk(otsv, dc, c)
        self.logger.info('done processing chunk %s-%d' % (k,i))


  def preload_data_samples(self):
    self.preload_by_type('data_samples', self.data_sample_klass,
                         self.preloaded_data_samples)

  def preload_data_collections(self):
    self.logger.info('start preloading data collections')
    ds = self.kb.get_objects(self.kb.DataCollection)
    for d in ds:
      self.preloaded_data_collections[d.label] = d
    self.logger.info('there are %d DataCollection(s) in the kb'
                     % len(self.preloaded_data_collections))

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks %s' % k)
    failures = 0
    #--
    for i, r in enumerate(records):
      reject = ' Rejecting import of row %d.' % i
      if not r['data_sample'] in self.preloaded_data_samples:
        f = 'bad data_sample in %s.' + reject
        self.logger.error( f % r['label'])
        failures += 1
        continue
    self.logger.info('done consistency checks %s' % k)

    return [] if failures else records

  def process_chunk(self, otsv, dc, chunk):
    items = []
    for r in chunk:
      conf = {'data_sample' : self.preloaded_data_samples[r['data_sample']],
              'dataCollection' : dc
              }
      items.append(self.kb.factory.create(self.kb.DataCollectionItem, conf))
    #--
    self.kb.save_array(items)

def canonize_records(args, records):
  fields = ['study', 'data_sample_type']
  for f in fields:
    if hasattr(args, f) and getattr(args,f) is not None:
      for r in records:
        r[f] = getattr(args, f)
  # specific hacks
  for r in records:
    if 'data_sample_type' not in r:
      r['data_sample_type'] = 'DataSample'

def make_parser_data_collection(parser):
  parser.add_argument('--study', type=str,
                      help="""default study used as context
                      for the import action.  It will
                      over-ride the study column value.""")
  parser.add_argument('--data_sample-type', type=str,
                      choices=['DataSample'],
                      help="""default datasample type.  It will
                      over-ride the data_sample_type column value, if any")


def import_data_collection_implementation(logger, args):

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

  o = csv.DictWriter(args.ofile,
                     fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t')
  o.writeheader()
  recorder.record(records, o)

  logger.info('done processing file %s' % args.ifile.name)


help_doc = """
import a new data collection definition into a virgil system.
"""

def do_register(registration_list):
  registration_list.append(('data_collection', help_doc,
                            make_parser_data_collection,
                            import_data_collection_implementation))

