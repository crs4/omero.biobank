# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import biosample
================

A biosample record will have, at least, the following fields::

  label     source
  I001-bs-2 V932814892
  I002-bs-2 V932814892
  I003-bs-2 None

Where label is the label of the biosample container. If a 'None' value
has been passed in the source column, the biosample will be imported
as a new unlinked object into the biobanks. Another example, this time
involving DNA samples::

  label    source     used_volume current_volume activation_date
  I001-dna V932814899 0.3         0.2            17/03/2007
  I002-dna V932814900 0.22        0.2            21/01/2004

A special case is when records refer to biosamples contained in plate
wells. In this case, an additional column must be present with the VID
of the corresponding TiterPlate object. For instance::

  plate  label source
  V39030 A01   V932814892
  V39031 A02   V932814893
  V39032 A03   V932814894

where the label column is now the label of the well position.

If row and column (optional) are provided, the program will use them;
if they are not provided, it will infer them from label (e.g., J01 ->
row=10, column=1). Missing labels will be generated as::

  '%s%03d' % (chr(row+ord('A')-1), column)

A badly formed label will result in the rejection of the record; the
same will happen if label, row and column are inconsistent. The well
will be filled by current_volume material produced by removing
used_volume material taken from the bio material contained in the
vessel identified by source. row and column are base 1.

If the sample is a IlluminaBeadChipArray the plate column used in the
PlateWell case will become a illumina_array column and a new column, named
bead_chip_assay_type, is required.

  illumina_array  label   source   bead_chip_assay_type
  V1351235        R01C01  V412441  HUMANEXOME_12V1_B
  V1351235        R01C02  V351151  HUMANEXOME_12V1_B
  V1351235        R02C01  V345115  HUMANEXOME_12V1_B


"""

import os, csv, json, time, re, copy, sys
import itertools as it
from datetime import datetime

from bl.vl.kb.drivers.omero.vessels import VesselContent, VesselStatus
from bl.vl.kb.drivers.omero.utils import make_unique_key
from bl.vl.kb.drivers.omero.illumina_chips import IlluminaBeadChipAssayType

import core
from version import version


class Recorder(core.Core):

  VESSEL_TYPE_CHOICES=['Tube', 'PlateWell', 'IlluminaBeadChipArray']
  SOURCE_TYPE_CHOICES=['Tube', 'Individual', 'PlateWell', 'NO_SOURCE']
  VESSEL_CONTENT_CHOICES=[x.enum_label() for x in VesselContent.__enums__]
  VESSEL_STATUS_CHOICES=[x.enum_label() for x in VesselStatus.__enums__]
  ICHIP_ASSAY_TYPE_CHOICES=[x.enum_label() for x in IlluminaBeadChipAssayType.__enums__]

  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               operator='Alfred E. Neumann', batch_size=10000,
               action_setup_conf=None, logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   logger=logger)
    self.operator = operator
    self.batch_size = batch_size
    self.action_setup_conf = action_setup_conf

  def record(self, records, otsv, rtsv, blocking_validation):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if not records:
      msg = 'No records are going to be imported'
      self.logger.warning(msg)
      sys.exit(0)
    study = self.find_study(records)
    self.source_klass = self.find_source_klass(records)
    self.vessel_klass = self.find_vessel_klass(records)
    records, bad_records = self.do_consistency_checks(records)
    for br in bad_records:
      rtsv.writerow(br)
    if blocking_validation and len(bad_records) >= 1:
      raise core.ImporterValidationError('%d invalid records' % len(bad_records))
    device = self.get_device('importer-%s.biosample' % version,
                             'CRS4', 'IMPORT', version)
    act_setups = set(Recorder.get_action_setup_options(r, self.action_setup_conf)
                     for r in records)
    asetup = {}
    for acts in act_setups:
    # asetup = self.get_action_setup('import-prog-%f' % time.time(),
    #                                json.dumps(self.action_setup_conf))
      setup_conf = {'label' : 'import-prog-%f' % time.time(),
                    'conf' : acts}
      setup = self.kb.factory.create(self.kb.ActionSetup, 
                                     setup_conf)
      asetup[acts] = self.kb.save(setup)
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(otsv, c, study, asetup, device)
      self.logger.info('done processing chunk %d' % i)

  def find_source_klass(self, records):
    try:
      return self.find_klass('source_type', records)
    except AttributeError:
      return None

  def find_vessel_klass(self, records):
    return self.find_klass('vessel_type', records)

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    if self.vessel_klass == self.kb.PlateWell:
      return self.do_consistency_checks_plate_well(records)
    elif self.vessel_klass == self.kb.IlluminaBeadChipArray:
      return self.do_consistency_checks_illumina_bead_chip(records)
    else:
      return self.do_consistency_checks_tube(records)

  def do_consistency_checks_plate_well(self, records, container_label='plate'):
    def build_key(r, container_label):
      container = self.kb.get_by_vid(self.kb.TiterPlate,
                                     r[container_label])
      return make_unique_key(container.label, r['label'])
    good_records = []
    bad_records = []
    grecs_keys = {}
    mandatory_fields = ['label', 'source', container_label, 'row', 'column']
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d: ' % i
      if self.missing_fields(mandatory_fields, r):
        f = 'missing mandatory field'
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if 'activation_date' in r and r['activation_date'] != '':
        try:
          datetime.strptime(r['activation_date'], '%d/%m/%Y')
        except ValueError:
          f = 'invalid date format for %s' % r['activation_date']
          self.logger.error(reject + f)
          bad_rec = copy.deepcopy(r)
          bad_rec['error'] = f
          bad_records.append(bad_rec)
          continue
      if r['source'] and not self.is_known_object_id(r['source'], self.source_klass):
        f = 'no known source with ID %s' % r['source']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if not self.is_known_object_id(r[container_label], self.kb.TiterPlate):
        f = 'no known container with ID %s' % r[container_label]
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      key = build_key(r, container_label)
      if self.is_known_object_key('containerSlotLabelUK', key, self.vessel_klass):
        f = 'there is a pre-existing vessel with label %s in container %s' % (r['label'],
                                                                              r[container_label])
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if key in grecs_keys:
        f = 'multiple records for label %s in container %s' % (r['label'], r[container_label])
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      good_records.append(r)
      grecs_keys[key] = i
    self.logger.info('done consistency checks')
    return good_records, bad_records

  def do_consistency_checks_illumina_bead_chip(self, records):
    good_records = []
    records, bad_records = self.do_consistency_checks_plate_well(records, 'illumina_array')
    mandatory_fields = ['bead_chip_assay_type']
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d.' % i
      if self.missing_fields(mandatory_fields, r):
        m = 'missing mandatory field. '
        self.logger.warning(m + reject)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = m
        bad_records.append(bad_rec)
        continue
      good_records.append(r)
    return good_records, bad_records

  def do_consistency_checks_tube(self, records):
    good_records = []
    bad_records = []
    grecs_labels = {}
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d.' % i
      if self.is_known_object_label(r['label'], self.vessel_klass):
        f = 'there is a pre-existing vessel with label %s' % r['label']
        self.logger.warn(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if 'activation_date' in r and r['activation_date'] != '':
        try:
          datetime.strptime(r['activation_date'], '%d/%m/%Y')
        except ValueError:
          f = 'invalid date format for %s' % r['activation_date']
          self.logger.error(reject + f)
          bad_rec = copy.deepcopy(r)
          bad_rec['error'] = f
          bad_records.append(bad_rec)
          continue
      if r['source'] and not self.is_known_object_id(r['source'], self.source_klass):
        f = 'no known source with ID %s. ' + r['source']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['label'] in grecs_labels:
        f = 'there is another vessel with label %s in this batch' % r['label']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
      good_records.append(r)
      grecs_labels[r['label']] = i
    self.logger.info('done consistency checks')
    return good_records, bad_records

  def process_chunk(self, otsv, chunk, study, asetup, device):
    aklass = {
      self.kb.Individual: self.kb.ActionOnIndividual,
      self.kb.Tube: self.kb.ActionOnVessel,
      self.kb.PlateWell: self.kb.ActionOnVessel,
      type(None) : self.kb.Action,
      }
    actions = []
    target_content = []
    for r in chunk:
      conf = {
        'setup': asetup[Recorder.get_action_setup_options(r, self.action_setup_conf)],
        'device': device,
        'actionCategory': getattr(self.kb.ActionCategory, r['action_category']),
        'operator': self.operator,
        'context': study,
        }
      if r['source']:
        target = self.kb.get_by_vid(self.source_klass, r['source'])
        conf['target'] = target
      else:
        target = None
      target_content.append(getattr(target, 'content', None))
      actions.append(self.kb.factory.create(aklass[target.__class__], conf))
    assert len(actions) == len(chunk)
    self.kb.save_array(actions)
    vessels = []
    for a, r, c in it.izip(actions, chunk, target_content):
      self.logger.debug(r)
      a.unload()
      current_volume = float(r['current_volume'])
      initial_volume = current_volume
      content = (c if r['action_category'] == 'ALIQUOTING' and c else
                 getattr(self.kb.VesselContent, r['vessel_content'].upper()))
      conf = {
        'label': r['label'],
        'currentVolume': current_volume,
        'initialVolume': initial_volume,
        'content': content,
        'status': getattr(self.kb.VesselStatus, r['vessel_status'].upper()),
        'action': a,
        }
      if 'activation_date' in r:
        conf['activationDate'] = time.mktime(datetime.strptime(r['activation_date'],
                                                               '%d/%m/%Y').timetuple())
      if self.vessel_klass == self.kb.PlateWell or \
          self.vessel_klass == self.kb.IlluminaBeadChipArray:
        if self.vessel_klass == self.kb.PlateWell:
          plate = self.kb.get_by_vid(self.kb.TiterPlate, r['plate'])
        else:
          plate = self.kb.get_by_vid(self.kb.IlluminaArrayOfArrays,
                                     r['illumina_array'])
          conf['assayType'] = getattr(self.kb.IlluminaBeadChipAssayType,
                                      r['bead_chip_assay_type'])
        row, column = r['row'], r['column']
        conf['container'] = plate
        conf['slot'] = (row - 1) * plate.columns + column
      elif 'barcode' in r:
        conf['barcode'] = r['barcode']
      self.logger.debug(conf)
      vessels.append(self.kb.factory.create(self.vessel_klass, conf))
    assert len(vessels) == len(chunk)
    self.kb.save_array(vessels)
    for v in vessels:
      otsv.writerow({
        'study': study.label,
        'label': v.label,
        'type': v.get_ome_table(),
        'vid': v.id,
        })


class RecordCanonizer(core.RecordCanonizer):

  WCORDS_PATTERN = re.compile(r'([a-z]+)(\d+)', re.IGNORECASE)
  ICHIPCORDS_PATTERN = re.compile(r'^r(\d{2})c(\d{2})$', re.IGNORECASE)
  
  def build_well_label(self, row, column, obj_klass):  # row and column are BASE 1
    if obj_klass == 'PlateWell':
      return '%s%02d' % (chr(row+ord('A')-1), column)
    elif obj_klass == 'IlluminaBeadChipArray':
      return 'R%02dC%02d' % (row, column)

  def find_well_coords(self, label, obj_klass):
    if obj_klass == 'PlateWell':
      m = self.WCORDS_PATTERN.match(label)
      if not m:
        raise ValueError('%s is not a valid well label' % label)
      row_label, col_label = m.groups()
      column = int(col_label)
      row = 0
      base = 1
      num_chars = ord('Z') - ord('A') + 1
      for x in row_label[::-1]:
        row += (ord(x)-ord('A')) * base
        base *= num_chars
      return row+1, column
    elif obj_klass == 'IlluminaBeadChipArray':
      m = self.ICHIPCORDS_PATTERN.match(label)
      if not m:
        raise ValueError('%s is not a valid illumina bead chip array label' % label)
      row_label, col_label = m.groups()
      return int(row_label), int(col_label)

  def canonize(self, r):
    super(RecordCanonizer, self).canonize(r)
    r.setdefault('action_category', 'IMPORT')
    for k in 'current_volume', 'used_volume':
      r.setdefault(k, 0.0)
    if r['source'] == 'None':
      r['source'] = None
    if r['vessel_type'] == 'PlateWell' or r['vessel_type'] == 'IlluminaBeadChipArray':
      if 'row' in r and 'column' in r:
        r['row'], r['column'] = map(int, [r['row'], r['column']])
        r.setdefault('label', self.build_well_label(r['row'], r['column']),
                     r['vessel_type'])
      elif 'label' in r:
        r['row'], r['column'] = self.find_well_coords(r['label'], r['vessel_type'])


def make_parser(parser):
  parser.add_argument('--study', metavar="STRING",
                      help="overrides the study column value")
  parser.add_argument('--action-category', metavar="STRING",
                      choices=['IMPORT', 'EXTRACTION', 'ALIQUOTING'],
                      help="overrides the action_category column value")
  parser.add_argument('--vessel-type', metavar="STRING",
                      choices=Recorder.VESSEL_TYPE_CHOICES,
                      help="overrides the vessel_type column value")
  parser.add_argument('--source-type', metavar="STRING",
                      choices=Recorder.SOURCE_TYPE_CHOICES,
                      help="overrides the source_type column value")
  parser.add_argument('--vessel-content', metavar="STRING",
                      choices=Recorder.VESSEL_CONTENT_CHOICES,
                      help="overrides the vessel_content column value")
  parser.add_argument('--vessel-status', metavar="STRING",
                      choices=Recorder.VESSEL_STATUS_CHOICES,
                      help="overrides the vessel_status column value")
  parser.add_argument('--current-volume', type=float, metavar="FLOAT",
                      help="overrides the current_volume column value")
  parser.add_argument('--used-volume', type=float, metavar="FLOAT",
                      help="overrides the used_volume column value")
  parser.add_argument('-N', '--batch-size', type=int, metavar="INT",
                      default=1000,
                      help="n. of objects to be processed at a time")
  parser.add_argument('--bead-chip-assay-type', metavar="STRING",
                      choices=Recorder.ICHIP_ASSAY_TYPE_CHOICES,
                      help='Illumina chip assay type (only used when importing IlluminaBeadChipArray objects)')


def implementation(logger, host, user, passwd, args, close_handles):
  fields_to_canonize = [
    'study',
    'source_type',
    'vessel_type',
    'vessel_content',
    'vessel_status',
    'current_volume',
    'used_volume',
    'action_category',
    ]
  if args.vessel_type == 'IlluminaBeadChipArray':
    fields_to_canonize.append('bead_chip_assay_type')
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(host=host, user=user, passwd=passwd,
                      keep_tokens=args.keep_tokens,
                      batch_size=args.batch_size, operator=args.operator,
                      action_setup_conf=action_setup_conf, logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  recorder.logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  canonizer = RecordCanonizer(fields_to_canonize, args)
  canonizer.canonize_list(records)
  o = csv.DictWriter(args.ofile,
                     fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t', lineterminator=os.linesep)
  o.writeheader()
  report_fnames = copy.deepcopy(f.fieldnames)
  report_fnames.append('error')
  report = csv.DictWriter(args.report_file, report_fnames,
                          delimiter='\t', lineterminator=os.linesep,
                          extrasaction='ignore')
  report.writeheader()
  records_map = Recorder.map_by_column(records, 'study')
  for study_label, records in records_map.iteritems():
    try:
      logger.info('Dumping %d records with study %s as reference', len(records), study_label)
      recorder.record(records, o, report,
                      args.blocking_validator)
    except core.ImporterValidationError as ve:
      recorder.logger.critical(ve.message)
      close_handles(args)
      raise ve
  close_handles(args)
  recorder.logger.info('done processing file %s' % args.ifile.name)


help_doc = """
import new biosample definitions into the KB and link them to
previously registered objects.
"""


def do_register(registration_list):
  registration_list.append(('biosample', help_doc, make_parser,
                            implementation))
