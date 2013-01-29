# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import data_collection
======================

Will read in a tsv file with the following columns::

  study    label data_sample
  BSTUDY   dc-01 V0390290
  BSTUDY   dc-01 V0390291
  BSTUDY   dc-02 V0390292
  BSTUDY   dc-02 V390293
  ...

This will create new DataCollection(s), whose label is defined by the
label column, and link to it, using DataCollectionItem objects,
the DataSample object identified by data_sample (a VID).

Records that point to an unknown DataSample will abort the data
collection loading. Previously seen collections will be noisily
ignored. It is not legal to use the importer to add items to a
previously known collection.
"""

import csv, json, time, os, copy
import itertools as it

from bl.vl.kb.drivers.omero.utils import make_unique_key

import core
from version import version


class Recorder(core.Core):
  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann',
               logger=None, action_setup_conf=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.batch_size = batch_size
    self.operator = operator
    self.action_setup_conf = action_setup_conf
    self.preloaded_data_samples = {}
    self.preloaded_data_collections = {}
    self.__preloaded_items = {}

  @property
  def preloaded_items(self):
    if not self.__preloaded_items:
      self.logger.info('start preloading data collection items')
      objs = self.kb.get_objects(self.kb.DataCollectionItem)
      for o in objs:
        assert not o.dataCollectionItemUK in self.__preloaded_items
        self.__preloaded_items[o.dataCollectionItemUK] = o
      self.logger.info('done preloading data collection items')
    return self.__preloaded_items

  def record(self, records, otsv, rtsv):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    def get_data_collection(label, action):
      if label in self.preloaded_data_collections:
        return self.preloaded_data_collections[label]
      else:
        dc_conf = {'label' : label, 'action': action}
        return self.kb.factory.create(self.kb.DataCollection, dc_conf)
    if len(records) == 0:
      self.logger.warn('no records')
      return
    study = self.find_study(records)
    self.data_sample_klass = self.find_data_sample_klass(records)
    self.preload_data_samples()
    self.preload_data_collections()
    asetup = self.get_action_setup('importer.data_collection-%f' % time.time(),
                                   json.dumps(self.action_setup_conf))
    device = self.get_device('importer-%s.data_collection' % version,
                             'CRS4', 'IMPORT', version)
    conf = {
      'setup': asetup,
      'device': device,
      'actionCategory': self.kb.ActionCategory.PROCESSING,
      'operator': self.operator,
      'context': study,
      }
    action = self.kb.factory.create(self.kb.Action, conf).save()
    def keyfunc(r): return r['label']
    sub_records = []
    data_collections = {}
    records = sorted(records, key=keyfunc)
    for k, g in it.groupby(records, keyfunc):
      data_collections[k] = get_data_collection(k, action)
      good_records, bad_records = self.do_consistency_checks(data_collections[k], list(g))
      sub_records.append(good_records)
      for br in bad_records:
        rtsv.writerow(br)
    records = sum(sub_records, [])
    if len(records) == 0:
      self.logger.warn('no records')
      self.kb.delete(action)
      return
    records = sorted(records, key=keyfunc)
    for k, g in it.groupby(records, keyfunc):
      dc = data_collections[k]
      if not dc.is_mapped():
        dc.save()
      for i, c in enumerate(records_by_chunk(self.batch_size, list(g))):
        self.logger.info('start processing chunk %s-%d' % (k, i))
        self.process_chunk(otsv, study, dc, c)
        self.logger.info('done processing chunk %s-%d' % (k,i))

  def find_data_sample_klass(self, records):
    return self.find_klass('data_sample_type', records)

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

  def do_consistency_checks(self, data_collection, records):
    self.logger.info('start consistency checks on %s' % data_collection.label)
    def build_key(dc, r):
      data_collection = dc
      data_sample = self.preloaded_data_samples[r['data_sample']]
      return make_unique_key(data_collection.id, data_sample.id)
    good_records = []
    bad_records = []
    seen = []
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d: ' % i
      if not r['data_sample'] in self.preloaded_data_samples:
        f = 'unknown data sample with ID %s' % r['data_sample']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['data_sample'] in seen:
        f = 'multiple copy of data_sample %s in this batch' % (r['data_sample'])
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      key = build_key(data_collection, r)
      if key in self.preloaded_items:
        f = 'data sample %s already in collection %s' % (r['data_sample'], data_collection.label)
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      seen.append(r['data_sample'])
      good_records.append(r)
    self.logger.info('done consistency checks on %s' % data_collection.label)
    return good_records, bad_records

  def process_chunk(self, otsv, study, dc, chunk):
    items = []
    for r in chunk:
      conf = {
        'dataSample': self.preloaded_data_samples[r['data_sample']],
        'dataCollection': dc,
        }
      items.append(self.kb.factory.create(self.kb.DataCollectionItem, conf))
    self.kb.save_array(items)
    otsv.writerow({
      'study': study.label,
      'label': dc.label,
      'type': dc.get_ome_table(),
      'vid': dc.id
      })


class RecordCanonizer(core.RecordCanonizer):
  
  def canonize(self, r):
    super(RecordCanonizer, self).canonize(r)
    r.setdefault('data_sample_type', 'DataSample')


def make_parser(parser):
  parser.add_argument('--study', metavar="STRING",
                      help="overrides the study column value")
  parser.add_argument('--data_sample-type', metavar="STRING",
                      choices=['DataSample'],
                      help="overrides the data_sample_type column value")
  parser.add_argument('--label', metavar="STRING",
                      help="overrides the label column value")


def implementation(logger, host, user, passwd, args):
  fields_to_canonize = ['study', 'data_sample_type', 'label']
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(args.study,
                      host=host, user=user, passwd=passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf, logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  canonizer = RecordCanonizer(fields_to_canonize, args)
  canonizer.canonize_list(records)
  o = csv.DictWriter(args.ofile,
                     fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t')
  o.writeheader()
  report_fnames = copy.deepcopy(f.fieldnames)
  report_fnames.append('error')
  report = csv.DictWriter(args.report_file, report_fnames,
                          delimiter='\t', lineterminator=os.linesep,
                          extrasaction='ignore')
  report.writeheader()
  recorder.record(records, o, report)
  args.ifile.close()
  args.ofile.close()
  args.report_file.close()
  logger.info('done processing file %s' % args.ifile.name)


help_doc = """
import a new data collection definition into the KB.
"""


def do_register(registration_list):
  registration_list.append(('data_collection', help_doc, make_parser,
                            implementation))
