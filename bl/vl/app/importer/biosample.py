"""
Import BioSample definitions
============================

A biosample record will have, at least, the following fields::

  label     source
  I001-bs-2 V932814892
  I002-bs-2 V932814892

Where label is the label of the biosample container. More info can be
provided through command line options. Example:

.. code-block:: bash

   ${IMPORT} ${SERVER_OPTS} -i bio_samples.tsv
                            -o bio_mapping.tsv biosample
                            --study  ${DEFAULT_STUDY}
                            --source-type Individual
                            --vessel-type Tube
                            --vessel-content BLOOD
                            --vessel-status  USABLE
                            --current-volume 20

where vessel-content and vessel-status must belong to the set of
possible values of, respectively, the VesselContent and VesselStatus
enums.

Another example, this time involving dna samples::

  label    source     used_volume current_volume
  I001-dna V932814899 0.3         0.2
  I002-dna V932814900 0.22        0.2

.. code-block:: bash

   ${IMPORT} ${SERVER_OPTS} -i bio_samples.tsv
                            -o bio_mapping.tsv biosample
                            --study  ${DEFAULT_STUDY}
                            --vessel-type Tube
                            --source-type Tube
                            --vessel-content DNA
                            --vessel-status  USABLE

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

The biosample sub-operation will output the vid ids of the created
biosample objects. The following is a complete example.

.. code-block:: bash

   bash> cat blood_sample.tsv
   label  individual_label
   I001-bs-2  I001
   I002-bs-2  I002
   I003-bs-2  I003
   I004-bs-2  I004
   I005-bs-2  I005
   I006-bs-2  I006
   bash> ${KB_QUERY} -o blood_sample_mapped.tsv
                map_vid -i blood_sample.tsv
                 --column individual_label
                 --source-type Individual
                 --study BSTUDY
   bash> cat blood_sample_mapped.tsv
   label  source
   I001-bs-2  V044DE795E7F9F42FEB9855288CF577A77
   I002-bs-2  V0B718B77691B145BFA8901FCCF6B37998
   I003-bs-2  V06C59B915C0FD47DABE6AE02C731780AF
   I004-bs-2  V080331A3E763348F4879A71FEAA11C699
   I005-bs-2  V00FE62DB1758648CFB91F354A7EF9AAE2
   I006-bs-2  V01654DCFC5BB640C0BB7EE088194E629D
   bash> ${IMPORTER} -i blood_sample_mapped.tsv -o blood_sample_mapping.tsv
             biosample
             --study BSTUDY --source-type Individual
             --vessel-content BLOOD --vessel-status CONTENTUSABLE
             --vessel-type Tube
   bash> cat blood_sample_mapping.tsv
   study  label type  vid
   BSTUDY I004-bs-2 Tube  V046B2FF2BF6D04B2A836B612FEE2AC2C3
   BSTUDY I006-bs-2 Tube  V04BB92ABFFDFF4D258118295BD28BC380
   BSTUDY I005-bs-2 Tube  V0AD3DDA422E11489D8FB75B5C338ADEA2
   BSTUDY I003-bs-2 Tube  V067F7669C11674A84824929AD115D54E2
   BSTUDY I001-bs-2 Tube  V0D3E2064DB1C742288E761B0E2AEBD7B8
   BSTUDY I002-bs-2 Tube  V05740B1A6AE844443B45CF1ED7A89C207

One more complete example for plate well import.

.. code-block:: bash

  ${KB_QUERY} -o plate_well_mapped_1.tsv
               map_vid -i plate_well.tsv
                   --column bio_sample_label
                   --source-type Tube
                   --study BSTUDY

  ${KB_QUERY} -o plate_well_mapped_2.tsv
               map_vid -i plate_well_mapped_1.tsv
                   --column plate_label,plate
                   --source-type TiterPlate
                   --study BSTUDY

  ${IMPORTER} -i plate_well_mapped_2.tsv -o plate_well_mapping.tsv
               biosample
               --study BSTUDY --source-type Tube --action-category ALIQUOTING
               --vessel-status CONTENTUSABLE --vessel-type PlateWell
"""

import os, csv, json, time, re
import itertools as it

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
    self.source_klass = self.find_source_klass(records)
    self.vessel_klass = self.find_vessel_klass(records)
    self.preload_sources()
    if self.vessel_klass == self.kb.PlateWell:
      self.preload_plates()
    records = self.do_consistency_checks(records)
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
    mandatory_fields = ['label', 'source', 'plate', 'row', 'column']
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d: ' % i
      if self.missing_fields(mandatory_fields, r):
        f = reject + 'missing mandatory field.'
        self.logger.error(f)
        continue
      if r['source'] not in  self.preloaded_sources:
        f = reject + 'no known source maps to %s.'
        self.logger.error(f % r['source'])
        continue
      if r['plate'] not in  self.preloaded_plates:
        f = reject + 'no known plate maps to %s.'
        self.logger.error(f % r['plate'])
        continue
      key = build_key(r)
      if key in self.preloaded_vessels:
        f = reject + 'there is a pre-existing vessel with key %s.'
        self.logger.warn(f % key)
        continue
      good_records.append(r)
    self.logger.info('done consistency checks')
    k_map = {}
    for r in good_records:
      key = build_key(r)
      if key in k_map:
        self.logger.error('multiple records for key %s' % key)
      else:
        k_map[key] = r
    return k_map.values()

  def do_consistency_checks_tube(self, records):
    def preload_vessels():
      self.logger.info('start preloading vessels')
      objs = self.kb.get_objects(self.vessel_klass)
      for o in objs:
        assert not o.label in self.preloaded_vessels
        self.preloaded_vessels[o.label] = o
      self.logger.info('done preloading vessels')
    k_map = {}
    for r in records:
      if r['label'] in k_map:
        self.logger.error('multiple records for label %s' % r['label'])
      else:
        k_map[r['label']] = r
    records = k_map.values()
    if len(records) == 0:
      return []
    preload_vessels()
    good_records = []
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d.' % i
      if r['label'] in self.preloaded_vessels:
        f = 'there is a pre-existing vessel with label %s. ' + reject
        self.logger.warn(f % r['label'])
        continue
      if not r['source'] in  self.preloaded_sources:
        f = 'no known source maps to %s. ' + reject
        self.logger.error(f % r['source'])
        continue
      good_records.append(r)
    self.logger.info('done consistency checks')
    return good_records

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


def implementation(logger, args):
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
  recorder = Recorder(host=args.host, user=args.user, passwd=args.passwd,
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
  recorder.record(records, o)
  args.ofile.close()
  recorder.logger.info('done processing file %s' % args.ifile.name)


help_doc = """
import new biosample definitions into the KB and link them to
previously registered objects.
"""


def do_register(registration_list):
  registration_list.append(('biosample', help_doc, make_parser,
                            implementation))
