"""
Import device
=============

Will read in a tsv file with the following columns (multiple spaces
represent tabs)::

  device_type  label   maker       model                        release
  Scanner      pula01  Affymetrix  GeneChip Scanner 3000        7G
  Chip         chip01  Affymetrix  Genome-Wide Human SNP Array  6.0

All devices have a type, a label, a maker, a model and a release;
optional fields are 'barcode' and 'location'.
"""

import csv, os
import core


class Recorder(core.Core):
  
  def __init__(self, study_label,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann',
               action_setup_conf=None, logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.batch_size = batch_size
    self.operator = operator
    self.action_setup_conf = action_setup_conf
    self.preloaded_devices = {}
    self.preloaded_markers_sets = {}
    self.known_barcodes = []

  def record(self, records, otsv):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if not records:
      self.logger.warn('no records')
      return
    study = self.find_study(records)
    self.preload_devices()
    self.preload_markers_sets()
    records = self.do_consistency_checks(records)
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(c, otsv, study)
      self.logger.info('done processing chunk %d' % i)

  def preload_devices(self):
    self.logger.info('start preloadind devices')
    devices = self.kb.get_objects(self.kb.Device)
    for d in devices:
      self.preloaded_devices[d.label] = d
      if hasattr(d, 'barcode') and d.barcode is not None:
        self.known_barcodes.append(d.barcode)
    self.logger.info('there are %d Device(s) in the kb'
                     % (len(self.preloaded_devices)))

  def preload_markers_sets(self):
    self.preload_by_type('markers_sets', self.kb.SNPMarkersSet,
                         self.preloaded_markers_sets)

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    k_map = {}
    good_records = []
    mandatory_fields = ['label', 'maker', 'model', 'release']
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d: ' % i
      if self.missing_fields(mandatory_fields, r):
        f = reject + 'missing mandatory field.'
        self.logger.error(f)
        continue
      if r['label'] in k_map:
        f = (reject +
             'there is a pre-existing device with label %s. (in this batch).')
        self.logger.error(f % r['label'])
        continue
      elif r['label'] in self.preloaded_devices:
        f = reject + 'there is a pre-existing device with label %s.'
        self.logger.warn(f % r['label'])
        continue
      if r['barcode'] and r['barcode'] in self.known_barcodes:
        m = reject + 'there is a pre-existing object with barcode %s.'
        self.logger.warn(m % r['barcode'])
        continue
      if 'device_type' not in r:
        f = reject + 'missing device_type for record with label %s.'
        self.logger.error(f % r['label'])
        continue
      elif not issubclass(getattr(self.kb, r['device_type']), self.kb.Device):
        f = (reject +
             'device_type of device label %s is not a subclass of Device')
        self.logger.error(f % r['label'])
        continue
      elif r['device_type'] == 'GenotypingProgram':
        if not r.get('markers_set', None):
          f = (reject +
               'device label %s is a GenotypingProgram, missing markers_set')
          self.logger.error(f % r['label'])
          continue
        elif r['markers_set'] not in self.preloaded_markers_sets:
          f = (reject +
               'device label %s is a GenotypingProgram, unknown markers_set')
          self.logger.error(f % r['label'])
          continue
      k_map['label'] = r
      good_records.append(r)
    self.logger.info('done consistency checks')
    return good_records

  def process_chunk(self, chunk, otsv, study):
    devices = []
    for r in chunk:
      dklass = getattr(self.kb, r['device_type'])
      conf = {}
      for k in ['label', 'maker', 'model', 'release']:
        conf[k] = r[k]
      if r['location']:
        conf['physicalLocation'] = r['location']
      if r['barcode']:
        conf['barcode'] = r['barcode']
      if r['markers_set']:
        conf['snpMarkersSet'] = self.preloaded_markers_sets[r['markers_set']]
      devices.append(self.kb.factory.create(dklass, conf))
    self.kb.save_array(devices)
    for d in devices:
      otsv.writerow({
        'study': study.label,
        'label': d.label,
        'type': d.get_ome_table(),
        'vid': d.id,
        })


class RecordCanonizer(core.RecordCanonizer):
  
  def canonize(self, r):
    super(RecordCanonizer, self).canonize(r)
    for f in ['location', 'barcode', 'markers_set']:
      if r.get(f, 'NONE').upper() == 'NONE':
        r[f] = None


help_doc = """
import new Device definitions into the KB.
"""


def make_parser(parser):
  parser.add_argument(
    '--study', type=str, metavar="STR",
    help="Study label. Overrides the study column value, if any"
    )
  parser.add_argument(
    '--device-type', type=str, metavar="STR",
    choices=['Chip', 'Scanner', 'SoftwareProgram', 'GenotypingProgram'],
    help="Device type. Overrides the device_type column value, if any"
    )
  parser.add_argument(
    '--maker', type=str, metavar="STR",
    help="Device maker. Overrides the maker column value"
    )
  parser.add_argument(
    '--model', type=str, metavar="STR",
    help="Device model. Overrides the model column value"
    )
  parser.add_argument(
    '--release', type=str, metavar="STR",
    help="Device release. Overrides the release column value"
    )


def implementation(logger, args):
  fields_to_canonize = ['study', 'maker', 'model', 'release', 'device_type']
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      keep_tokens=args.keep_tokens, logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  args.ifile.close()
  canonizer = RecordCanonizer(fields_to_canonize, args)
  canonizer.canonize_list(records)
  o = csv.DictWriter(args.ofile, delimiter='\t',
                     fieldnames=['study', 'label', 'type', 'vid'],
                     lineterminator=os.linesep)
  o.writeheader()
  recorder.record(records, o)
  args.ofile.close()
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('device', help_doc, make_parser, implementation))
