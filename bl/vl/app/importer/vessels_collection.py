# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import data_collection
======================

Will read in a tsv file with the following columns::

  study    label vessel   vessel_type
  BSTUDY   dc-01 V0390290 Vessel
  BSTUDY   dc-01 V0390291 Vessel
  BSTUDY   dc-02 V0390292 Vessel
  BSTUDY   dc-02 V390293  Vessel
  ...

This will create new VesselCollection(s), whose label is defined by
the label column, and link to it, using DataCollectionItem objects,
the DataSample object identified by vessel (a VID).

Records that point to an unknown Vessel will abort the vessels
collection loading. Previously seen collections will be noisily
ignored. It is not legal to use the importer to add items to a
previously known collection.
"""

import csv, json, time, copy
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
    self.preloaded_vessels = {}
    self.preloaded_vessels_collections = {}
    self.preloaded_items = {}

  def record(self, records, otsv, rtsv, blocking_validation):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    def get_vessels_collection(label, action):
      if label in self.preloaded_vessels_collections:
        return self.preloaded_vessels_collections[label]
      else:
        vc_conf = {'label' : label, 'action': action}
        return self.kb.factory.create(self.kb.VesselsCollection, vc_conf)
    if len(records) == 0:
      msg = 'No records are going to be imported'
      self.logger.critical(msg)
      raise core.ImporterValidationError(msg)
    study = self.find_study(records)
    self.vessel_klass = self.find_vessel_klass(records)
    self.preload_vessels()
    self.preload_vessels_collections()
    asetup = self.get_action_setup('importer.vessels_collection-%f' % time.time(),
                                   json.dumps(self.action_setup_conf))
    device = self.get_device('importer-%s.vessels_collection' % version,
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
    vessels_collections = {}
    records = sorted(records, key=keyfunc)
    for k, g in it.groupby(records, keyfunc):
      vessels_collections[k] = get_vessels_collection(k, action)
      good_records, bad_records = self.do_consistency_checks(vessels_collections[k], list(g))
      sub_records.append(good_records)
      for br in bad_records:
        rtsv.writerow(br)
      if blocking_validation and len(bad_records) >= 1:
        self.kb.delete(action)
        raise core.ImporterValidationError('%d invalid records' % len(bad_records))
    records = sum(sub_records, [])
    records = sorted(records, key=keyfunc)
    for k, g in it.groupby(records, keyfunc):
      vc = vessels_collections[k]
      if not vc.is_mapped():
        vc.save()
      for i, c in enumerate(records_by_chunk(self.batch_size, list(g))):
        self.logger.info('start processing chunk %s-%d' % (k, i))
        self.process_chunk(otsv, study, vc, c)
        self.logger.info('done processing chunk %s-%d' % (k,i))

  def find_vessel_klass(self, records):
    return self.find_klass('vessel_type', records)

  def preload_vessels(self):
    self.preload_by_type('vessel', self.vessel_klass,
                         self.preloaded_vessels)

  def preload_vessels_collections(self):
    self.logger.info('start preloading vessels collections')
    vs = self.kb.get_objects(self.kb.VesselsCollection)
    for v in vs:
      self.preloaded_vessels_collections[v.label] = v
    self.logger.info('there are %d VesselsCollection(s) in the kb'
                     % len(self.preloaded_vessels_collections))

  def do_consistency_checks(self, vessels_collection, records):
    def preload_vessels_collection_items():
      self.logger.info('start preloading vessels collection items')
      objs = self.kb.get_objects(self.kb.VesselsCollectionItem)
      for o in objs:
        assert not o.vesselsCollectionItemUK in self.preloaded_items
        self.preloaded_items[o.vesselsCollectionItemUK] = o
      self.logger.info('done preloading vessels collection items')
    self.logger.info('start consistency checks on %s' % vessels_collection.label)
    def build_key(vc, r):
      vessels_collection = vc
      vessel = self.preloaded_vessels[r['vessel']]
      return make_unique_key(vessels_collection.id, vessel.id)
    preload_vessels_collection_items()
    good_records = []
    bad_records = []
    seen = []
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d: ' % i
      if not r['vessel'] in self.preloaded_vessels:
        f = 'there is no known vessel with ID %s' % r['vessel']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['vessel'] in seen:
        f = 'multiple copy of the same vessel %s in %s in this batch' % (r['vessel'], i)
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      key = build_key(vessels_collection, r)
      if key in self.preloaded_items:
        f = 'vessel %s already in collection %s' % (r['vessel'], i)
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      seen.append(r['vessel'])
      good_records.append(r)
    self.logger.info('done consistency checks on %s' % vessels_collection.label)
    return good_records, bad_records

  def process_chunk(self, otsv, study, vc, chunk):
    items = []
    for r in chunk:
      conf = {
        'vessel': self.preloaded_vessels[r['vessel']],
        'vesselsCollection': vc,
        }
      items.append(self.kb.factory.create(self.kb.VesselsCollectionItem, conf))
    self.kb.save_array(items)
    otsv.writerow({
      'study': study.label,
      'label': vc.label,
      'type': vc.get_ome_table(),
      'vid': vc.id
      })


class RecordCanonizer(core.RecordCanonizer):
  
  def canonize(self, r):
    super(RecordCanonizer, self).canonize(r)
    r.setdefault('vessel_type', 'Vessel')


def make_parser(parser):
  parser.add_argument('--study', metavar="STRING",
                      help="overrides the study column value")
  parser.add_argument('--vessel-type', metavar="STRING",
                      choices=['Vessel'],
                      help="overrides the vessel_type column value")
  parser.add_argument('--label', metavar="STRING",
                      help="overrides the label column value")


def implementation(logger, host, user, passwd, args, close_handles):
  fields_to_canonize = ['study', 'vessel_type', 'label']
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
                          delimiter='\t', extrasaction='ignore')
  report.writeheader()
  try:
    recorder.record(records, o, report,
                    args.blocking_validator)
  except core.ImporterValidationError as ve:
    logger.critical(ve.message)
    raise
  close_handles(args)
  logger.info('done processing file %s' % args.ifile.name)


help_doc = """
import a new vessels collection definition into the KB.
"""


def do_register(registration_list):
  registration_list.append(('vessels_collection', help_doc, make_parser,
                            implementation))
