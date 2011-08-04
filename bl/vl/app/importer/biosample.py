"""
FIXME

A biosample record will have, at least, the following fields::

  label     source
  I001-bs-2 V932814892
  I002-bs-2 V932814892


.. code-block:: bash
   ${IMPORT} ${SERVER_OPTS} -i bio_samples.tsv
                            -o bio_mapping.tsv biosample \
                            --study  ${DEFAULT_STUDY} \
                            --source-type Individual \
                            --vessel-type Tube \
                            --vessel-content BLOOD \
                            --vessel-status  USABLE \
                            --current-volume 20

where vessel-content is taken from the enum VesselContent possible
values and vessel-status from the enum VesselStatus

Another example, this time dna samples::

  label    source     used_volume current_volume
  I001-dna V932814899 0.3         0.2
  I002-dna V932814900 0.22        0.2

where '''used_volume''' and '''current_volume''' are measured in FIXME
microliters.

.. code-block:: bash

   ${IMPORT} ${SERVER_OPTS} -i bio_samples.tsv
                            -o bio_mapping.tsv biosample \
                            --study  ${DEFAULT_STUDY} \
                            --vessel-type Tube \
                            --source-type Tube \
                            --vessel-content DNA \
                            --vessel-status  USABLE


A special case is when the records refer to biosamples contained in
plate wells. Together with the minimal columns above, there should be
a column with the vid of the relevant TiterPlate. For instance::

  plate  label source
  V39030 A01   V932814892
  V39031 A02   V932814893
  V39032 A03   V932814894

where the label column is now the label of the well position.

If row and column (optional) are provided, it will use that
location. If they are not, it will deduce them from label (e.g., J01
-> row=10, column=1). Missing labels will be generated as

       '%s%03d' % (chr(row + ord('A') - 1), column)

Badly formed label will bring the rejection of the record. The same
will happen if label, row and column are inconsistent.  The well will
be filled by current_volume material produced by removing used_volume
material taken from the bio material contained in the vessel
identified by source. Row and Column are base 1.

"""

from core import Core, BadRecord

from version import version

import csv, json, time

import itertools as it

# FIXME this is an hack...
from bl.vl.kb.drivers.omero.vessels import VesselContent, VesselStatus
from bl.vl.kb.drivers.omero.utils import make_unique_key


class Recorder(Core):
  """
  A class that helps in the recording of BioSamples subclasses into VL
  """
  VESSEL_TYPE_CHOICHES=['Tube', 'PlateWell']
  SOURCE_TYPE_CHOICHES=['Tube', 'Individual', 'PlateWell']
  VESSEL_CONTENT_CHOICES=[x.enum_label() for x in VesselContent.__enums__]
  VESSEL_STATUS_CHOICES=[x.enum_label() for x in VesselStatus.__enums__]


  # FIXME: the klass_name thing is a kludge...
  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               operator='Alfred E. Neumann', batch_size=10000,
               action_setup_conf=None, logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
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
    #--
    if not records:
      self.logger.warn('no records')
      return
    #--
    study                = self.find_study(records)
    self.source_klass    = self.find_source_klass(records)
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
      return make_unique_key(self.preloaded_plates[r['plate']].label,
                             r['label'])
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
        self.logger.error('multiple record for the same key: %s. Rejecting.'
                          % key)
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
        self.logger.error('multiple record for the same label: %s. Rejecting.'
                          % r['label'])
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
    #--
    return good_records

  def process_chunk(self, otsv, chunk,
                    study, asetup, device):
    aklass = {self.kb.Individual : self.kb.ActionOnIndividual,
              self.kb.Tube       : self.kb.ActionOnVessel,
              self.kb.PlateWell  : self.kb.ActionOnVessel,
              }
    actions = []
    target_content = []
    for r in chunk:
      target = self.preloaded_sources[r['source']]
      target_content.append(target.content if hasattr(target, 'content')
                            else None)
      conf = {'setup' : asetup,
              'device': device,
              'actionCategory' : getattr(self.kb.ActionCategory,
                                         r['action_category']),
              'operator' : self.operator,
              'context'  : study,
              'target' : target
              }
      actions.append(self.kb.factory.create(aklass[target.__class__], conf))
    assert len(actions) == len(chunk)
    self.kb.save_array(actions)
    #--
    vessels = []
    for a,r,c in it.izip(actions, chunk, target_content):
      a.unload()
      current_volume = float(r['current_volume'])
      initial_volume = current_volume
      content = (c if r['action_category'] == 'ALIQUOTING'
                   else getattr(self.kb.VesselContent,
                                r['vessel_content'].upper()))
      conf = {
        'label'         : r['label'],
        'currentVolume' : current_volume,
        'initialVolume' : initial_volume,
        'content' : content,
        'status'  : getattr(self.kb.VesselStatus,
                            r['vessel_status'].upper()),
        'action'        : a,
        }
      if self.vessel_klass == self.kb.PlateWell:
        plate = self.preloaded_plates[r['plate']]
        row, column = r['row'], r['column']
        conf['container'] = plate
        conf['slot']      = (row - 1) * plate.columns + column
      elif 'barcode' in r:
        conf['barcode'] = r['barcode']
      vessels.append(self.kb.factory.create(self.vessel_klass, conf))
    #--
    assert len(vessels) == len(chunk)
    self.kb.save_array(vessels)
    for v in vessels:
      otsv.writerow({'study' : study.label,
                     'label' : v.label,
                     'type'  : v.get_ome_table(),
                     'vid'   : v.id })

def canonize_records(args, records):
  def build_well_label(row, column):
    # row and column are BASE 1 !
    return '%s%02d' % (chr(row + ord('A') - 1), column)

  def find_well_coords(label):
    # FIXME this is ugly, but who cares...
    for i in range(len(label)-1, 0, -1):
      if not label[i].isdigit():
        break
    column = int(label[i+1:])
    label = label[:i+1]
    row  = 0
    base = 1
    for x in label[::-1]:
      row += (ord(x)-ord('A') + 1) * base
      base *= 26
    return row, column

  fields = ['study', 'source_type',
            'vessel_type', 'vessel_content', 'vessel_status',
            'current_volume', 'used_volume', 'action_category']
  for f in fields:
    if hasattr(args, f) and getattr(args,f) is not None:
      for r in records:
        r[f] = getattr(args, f)

  # handle special cases
  for r in records:
    if 'action_category' not in r:
      r['action_category'] = 'IMPORT'

  if r['vessel_type'] == 'PlateWell':
    for r in records:
      if ('row' in r and 'column' in r):
        r['row'], r['column'] = map(int, [r['row'],r['column']])
        if 'label' not in r:
          r['label'] = build_well_label(r['row'], r['column'])
      elif 'label' in r:
        r['row'], r['column'] = find_well_coords(r['label'])


def make_parser_biosample(parser):
  parser.add_argument('--study', type=str,
                      help="""default study assumed as context for the
                      import action.  It will
                      over-ride the study column value, if any.""")
  parser.add_argument('--action-category', type=str,
                      choices=['IMPORT', 'EXTRACTION', 'ALIQUOTING'],
                      help="""default action category.
                      It will over-ride the action_category column
                      value, if any. It will default to IMPORT""")
  parser.add_argument('--vessel-type', type=str,
                      choices=Recorder.VESSEL_TYPE_CHOICHES,
                      help="""default vessel type.  It will
                      over-ride the vessel_type column value, if any.
                      """)
  parser.add_argument('--source-type', type=str,
                      choices=Recorder.SOURCE_TYPE_CHOICHES,
                      help="""default source type.  It will
                      over-ride the source_type column value, if any.
                      """)
  parser.add_argument('--vessel-content', type=str,
                      choices=Recorder.VESSEL_CONTENT_CHOICHES,
                      help="""default vessel content.  It will
                      over-ride the vessel_column value, if any.
                      """)
  parser.add_argument('--vessel-status', type=str,
                      choices=Recorder.VESSEL_STATUS_CHOICHES,
                      help="""default vessel status.  It will
                      over-ride the vessel_status column value, if any.
                      """)
  parser.add_argument('--current-volume', type=float,
                      help="""default current volume assigned to
                      the sample.
                      It will over-ride the current_volume column value.""",
                      default=0.0)
  parser.add_argument('--used-volume', type=float,
                      help="""default used volume that was needed to create
                      the biosample.
                      It will over-ride the used_volume column value.""",
                      default=0.0)
  parser.add_argument('-N', '--batch-size', type=int,
                      help="""Size of the batch of objects
                      to be processed in parallel (if possible)""",
                      default=1000)

def import_biosample_implementation(logger, args):

  action_setup_conf = Recorder.find_action_setup_conf(args)

  recorder = Recorder(host=args.host, user=args.user,
                      passwd=args.passwd,
                      keep_tokens=args.keep_tokens,
                      batch_size=args.batch_size,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      logger=logger)
  #--
  f = csv.DictReader(args.ifile, delimiter='\t')
  recorder.logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  #--
  canonize_records(args, records)
  #--
  #--
  o = csv.DictWriter(args.ofile,
                     fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t')
  o.writeheader()
  recorder.record(records, o)
  #--
  recorder.logger.info('done processing file %s' % args.ifile.name)

help_doc = """
import new biosample definitions into a virgil system and link
them to previously registered objects.
"""

def do_register(registration_list):
  registration_list.append(('biosample', help_doc,
                            make_parser_biosample,
                            import_biosample_implementation))


