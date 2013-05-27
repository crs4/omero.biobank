# BEGIN_COPYRIGHT
# END_COPYRIGHT

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

import csv, os, copy
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

  def record(self, records, otsv, rtsv, blocking_validation):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if not records:
      msg = 'No records are going to be imported'
      self.logger.critical(msg)
      raise core.ImporterValidationError(msg)
    study = self.find_study(records)
    self.preload_devices()
    self.preload_markers_sets()
    records, bad_records = self.do_consistency_checks(records)
    for br in bad_records:
      rtsv.writerow(br)
    if blocking_validation and len(bad_records) >= 1:
      raise core.ImporterValidationError('%d invalid records' % len(bad_records))
    if not records:
      msg = 'No records are going to be imported'
      self.logger.critical(msg)
      raise core.ImporterValidationError(msg)
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
    b_map = {}
    good_records = []
    bad_records = []
    mandatory_fields = ['label', 'maker', 'model', 'release']
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d: ' % i
      if self.missing_fields(mandatory_fields, r):
        f = 'missing mandatory field'
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['label'] in k_map:
        f = 'there is a pre-existing device with label %s in this batch' % r['label']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      elif r['label'] in self.preloaded_devices:
        f = 'there is a pre-existing device with label %s' % r['label']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['barcode'] and r['barcode'] in self.known_barcodes:
        f = 'there is a pre-existing object with barcode %s' % r['barcode']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['barcode'] and r['barcode'] in b_map:
        f = 'there is a pre-existing device with barcode %s in this batch' % r['barcode']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if 'device_type' not in r:
        f = 'missing device_type for record'
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      elif not issubclass(getattr(self.kb, r['device_type']), self.kb.Device):
        f = '%s is not a legal device type' % r['device_type']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      elif r['device_type'] == 'GenotypingProgram':
        if not r.get('markers_set', None):
          f = ' missing markers_set'
          self.logger.error(reject + f)
          bad_rec = copy.deepcopy(r)
          bad_rec['error'] = f
          bad_records.append(bad_rec)
          continue
        elif r['markers_set'] not in self.preloaded_markers_sets:
          f = 'there is no markers set with ID %s' % r['markers_set']
          self.logger.error(reject + f)
          bad_rec = copy.deepcopy(r)
          bad_rec['error'] = f
          bad_records.append(bad_rec)
          continue
      k_map[r['label']] = r
      b_map[r['barcode']] = r
      good_records.append(r)
    self.logger.info('done consistency checks')
    return good_records, bad_records

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


def implementation(logger, host, user, passwd, args):
  fields_to_canonize = ['study', 'maker', 'model', 'release', 'device_type']
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(args.study,
                      host=host, user=user, passwd=passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      keep_tokens=args.keep_tokens, logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  canonizer = RecordCanonizer(fields_to_canonize, args)
  canonizer.canonize_list(records)
  o = csv.DictWriter(args.ofile, delimiter='\t',
                     fieldnames=['study', 'label', 'type', 'vid'],
                     lineterminator=os.linesep)
  o.writeheader()
  report_fnames = copy.deepcopy(f.fieldnames)
  report_fnames.append('error')
  report = csv.DictWriter(args.report_file, report_fnames,
                          delimiter='\t', lineterminator=os.linesep,
                          extrasaction='ignore')
  report.writeheader()
  try:
    recorder.record(records, o, report,
                    args.blocking_validator)
  except core.ImporterValidationError, ve:
    args.ifile.close()
    args.ofile.close()
    args.report_file.close()
    logger.critical(ve.message)
    raise ve
  args.ifile.close()
  args.ofile.close()
  args.report_file.close()
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('device', help_doc, make_parser, implementation))
