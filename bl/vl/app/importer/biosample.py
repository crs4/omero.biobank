# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import biosample
================

A biosample record will have, at least, the following fields::

  label     source
  I001-bs-2 V932814892
  I002-bs-2 V932814892

Where label is the label of the biosample container. Another example,
this time involving DNA samples::

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
"""

import os, csv, json, time, re, copy
import itertools as it
from datetime import datetime

from bl.vl.kb.drivers.omero.vessels import VesselContent, VesselStatus
from bl.vl.kb.drivers.omero.utils import make_unique_key

import core
from version import version


class Recorder(core.Core):

  VESSEL_TYPE_CHOICES=['Tube', 'PlateWell']
  SOURCE_TYPE_CHOICES=['Tube', 'Individual', 'PlateWell']
  VESSEL_CONTENT_CHOICES=[x.enum_label() for x in VesselContent.__enums__]
  VESSEL_STATUS_CHOICES=[x.enum_label() for x in VesselStatus.__enums__]

  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               operator='Alfred E. Neumann', batch_size=10000,
               action_setup_conf=None, logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   logger=logger)
    self.operator = operator
    self.batch_size = batch_size
    self.action_setup_conf = action_setup_conf
    self.preloaded_sources = {}
    self.preloaded_plates = {}
    self.preloaded_vessels = {}

  def record(self, records, otsv, rtsv):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if not records:
      self.logger.warn('no records')
      return
    study = self.find_study(records)
    self.source_klass = self.find_source_klass(records)
    self.vessel_klass = self.find_vessel_klass(records)
    self.preload_sources()
    if self.vessel_klass == self.kb.PlateWell:
      self.preload_plates()
    records, bad_records = self.do_consistency_checks(records)
    for br in bad_records:
      rtsv.writerow(br)
    device = self.get_device('importer-%s.biosample' % version,
                             'CRS4', 'IMPORT', version)
    asetup = self.get_action_setup('import-prog-%f' % time.time(),
                                   json.dumps(self.action_setup_conf))
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(otsv, c, study, asetup, device)
      self.logger.info('done processing chunk %d' % i)

  def find_source_klass(self, records):
    return self.find_klass('source_type', records)

  def find_vessel_klass(self, records):
    return self.find_klass('vessel_type', records)

  def preload_sources(self):
    self.preload_by_type('sources', self.source_klass, self.preloaded_sources)

  def preload_plates(self):
    self.preload_by_type('plates', self.kb.TiterPlate, self.preloaded_plates)

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    if self.vessel_klass == self.kb.PlateWell:
      return self.do_consistency_checks_plate_well(records)
    else:
      return self.do_consistency_checks_tube(records)

  def do_consistency_checks_plate_well(self, records):
    def preload_vessels():
      self.logger.info('start preloading vessels')
      objs = self.kb.get_objects(self.vessel_klass)
      for o in objs:
        assert not o.containerSlotLabelUK in self.preloaded_vessels
        self.preloaded_vessels[o.containerSlotLabelUK] = o
      self.logger.info('done preloading vessels')
    def build_key(r):
      plate = self.preloaded_plates[r['plate']]
      return make_unique_key(plate.label, r['label'])
    preload_vessels()
    good_records = []
    bad_records = []
    grecs_keys = {}
    mandatory_fields = ['label', 'source', 'plate', 'row', 'column']
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
      if r['source'] not in  self.preloaded_sources:
        f = 'no known source with ID %s' % r['source']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['plate'] not in  self.preloaded_plates:
        f = 'no known plate with ID %s' % r['plate']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      key = build_key(r)
      if key in self.preloaded_vessels:
        f = 'there is a pre-existing vessel with label %s in plate %s' % (r['label'],
                                                                          r['plate'])
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if key in grecs_keys:
        f = 'multiple records for label %s in plate %s' % (r['label'], r['plate'])
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      good_records.append(r)
      grecs_keys[key] = i
    self.logger.info('done consistency checks')
    return good_records, bad_records

  def do_consistency_checks_tube(self, records):
    def preload_vessels():
      self.logger.info('start preloading vessels')
      objs = self.kb.get_objects(self.vessel_klass)
      for o in objs:
        assert not o.label in self.preloaded_vessels
        self.preloaded_vessels[o.label] = o
      self.logger.info('done preloading vessels')
    preload_vessels()
    good_records = []
    bad_records = []
    grecs_labels = {}
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d.' % i
      if r['label'] in self.preloaded_vessels:
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
      if not r['source'] in  self.preloaded_sources:
        f = 'no known source with ID %s. ' + r['source']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['label'] in grecs_labels:
        f = 'there is a pre-existing vessel with label %s' % r['label']
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
      }
    actions = []
    target_content = []
    for r in chunk:
      target = self.preloaded_sources[r['source']]
      target_content.append(getattr(target, 'content', None))
      conf = {
        'setup': asetup,
        'device': device,
        'actionCategory': getattr(self.kb.ActionCategory, r['action_category']),
        'operator': self.operator,
        'context': study,
        'target': target
        }
      actions.append(self.kb.factory.create(aklass[target.__class__], conf))
    assert len(actions) == len(chunk)
    self.kb.save_array(actions)
    vessels = []
    for a, r, c in it.izip(actions, chunk, target_content):
      a.unload()
      current_volume = float(r['current_volume'])
      initial_volume = current_volume
      content = (c if r['action_category'] == 'ALIQUOTING' else
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
        conf['activationDate'] = time.mktime(datetime.strptime(r['activation_date'], '%d/%m/%Y').timetuple())
      if self.vessel_klass == self.kb.PlateWell:
        plate = self.preloaded_plates[r['plate']]
        row, column = r['row'], r['column']
        conf['container'] = plate
        conf['slot'] = (row - 1) * plate.columns + column
      elif 'barcode' in r:
        conf['barcode'] = r['barcode']
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
  
  def build_well_label(self, row, column):  # row and column are BASE 1
    return '%s%02d' % (chr(row+ord('A')-1), column)

  def find_well_coords(self, label):
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

  def canonize(self, r):
    super(RecordCanonizer, self).canonize(r)
    r.setdefault('action_category', 'IMPORT')
    for k in 'current_volume', 'used_volume':
      r.setdefault(k, 0.0)
    if r['vessel_type'] == 'PlateWell':
      if 'row' in r and 'column' in r:
        r['row'], r['column'] = map(int, [r['row'], r['column']])        
        r.setdefault('label', self.build_well_label(r['row'], r['column']))
      elif 'label' in r:
        r['row'], r['column'] = self.find_well_coords(r['label'])


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


def implementation(logger, host, user, passwd, args):
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
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(host=host, user=user, passwd=passwd,
                      keep_tokens=args.keep_tokens,
                      batch_size=args.batch_size, operator=args.operator,
                      action_setup_conf=action_setup_conf, logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  recorder.logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  args.ifile.close()
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
  recorder.record(records, o, report)
  args.ifile.close()
  args.ofile.close()
  args.report_file.close()
  recorder.logger.info('done processing file %s' % args.ifile.name)


help_doc = """
import new biosample definitions into the KB and link them to
previously registered objects.
"""


def do_register(registration_list):
  registration_list.append(('biosample', help_doc, make_parser,
                            implementation))
