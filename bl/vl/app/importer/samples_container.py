# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import biosample container
==========================

TODO: add doc
....
"""

import os, csv, json, copy, time
import itertools as it
from datetime import datetime

from bl.vl.kb.drivers.omero.objects_collections import ContainerStatus

import core
from version import version


class Recorder(core.Core):

  CONTAINER_TYPE_CHOICES = ['TiterPlate', 'FlowCell', 'Lane']
  STATUS_CHOICES = [x.enum_label() for x in ContainerStatus.__enums__]

  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann',
               action_setup_conf=None, logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.batch_size = batch_size
    self.operator = operator
    self.action_setup_conf = action_setup_conf
    self.preloaded_flowcells = {}

  def record(self, records, otsv, rtsv):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if not records:
      self.logger.warn('no records')
      return
    self.container_klass = self.find_container_klass(records)
    self.preload_containers()
    if self.container_klass == self.kb.Lane:
      self.preload_flowcells()
    records, bad_records = self.do_consistency_checks(records)
    for br in bad_records:
      rtsv.writerow(br)
    if len(records) == 0:
      return
    study = self.find_study(records)
    device = self.get_device(label='importer-%s.titer_plate' % version,
                             maker='CRS4', model='importer', release=version)
    asetup = self.get_action_setup('importer.dna_sample',
                                   json.dumps(self.action_setup_conf))
    acat = self.kb.ActionCategory.IMPORT
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(otsv, c, study, asetup, device, acat)
      self.logger.info('done processing chunk %d' % i)

  def find_container_klass(self, records):
    return self.find_klass('container_type', records)

  def preload_flowcells(self):
    self.preload_by_type('flowcells', self.kb.FlowCell, self.preloaded_flowcells)

  def preload_containers(self):
    self.logger.info('start prefetching containers')
    self.known_containers = {}
    self.known_barcodes = []
    containers = self.kb.get_objects(self.kb.Container)
    for c in containers:
      self.known_containers[c.label] = c
      if hasattr(c, 'barcode') and c.barcode is not None:
        self.known_barcodes.append(c.barcode)
    self.logger.info('there are %d objects in the kb' %
                     (len(self.known_containers)))

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    good_recs, bad_recs = self.do_consistency_checks_common_fields(records)
    if self.container_klass == self.kb.TiterPlate:
      good_recs, brecs = self.do_consistency_checks_titer_plate(good_recs)
      bad_recs.extend(brecs)
    elif self.container_klass == self.kb.FlowCell:
      good_recs, brecs = self.do_consistency_checks_flow_cell(good_recs)
      bad_recs.extend(brecs)
    elif self.container_klass == self.kb.Lane:
      good_recs, brecs = self.do_consistency_checks_lane(good_recs)
      bad_recs.extend(brecs)
    self.logger.info('done consistency checks')
    return good_recs, bad_recs

  def do_consistency_checks_common_fields(self, records):
    good_records = []
    bad_records = []
    grecs_barcodes = {}
    grecs_labels = {}
    mandatory_fields = ['label', 'container_status']
    for i, r in enumerate(records):
      reject = 'Rejecting import of line %d.' % i
      if self.missing_fields(mandatory_fields, r):
        m = 'missing mandatory field. '
        self.logger.warning(m + reject)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = m
        bad_records.append(bad_rec)
        continue
      if r['label'] in grecs_labels:
        m = 'label %s already used in record %d. ' % (r['label'],
                                                      grecs_labels[r['label']])
        self.logger.warning(m + reject)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = m
        bad_records.append(bad_rec)
        continue
      if r['barcode'] and r['barcode'] in grecs_barcodes:
        m = 'barcode %s already used in record %d. ' % (r['barcode'],
                                                        grecs_barcodes[r['barcode']])
        self.logger.warn(m + reject)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = m
        bad_records.append(bad_rec)
        continue
      if r['barcode'] and r['barcode'] in self.known_barcodes:
        m = 'there is a pre-existing object with barcode %s. ' % r['barcode']
        self.logger.warn(m + reject)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = m
        bad_records.append(bad_rec)
        continue
      if self.known_containers.has_key(r['label']):
        m = 'there is a pre-existing object with label %s. ' % r['label']
        self.logger.warn(m + reject)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = m
        bad_records.append(bad_rec)
        continue
      if 'creation_date' in r and r['creation_date'] != '':
        try:
          datetime.strptime(r['creation_date'], '%d/%m/%Y')
        except ValueError:
          m = 'invalid date format for %s' % r['creation_date']
          self.logger.warn(m + reject)
          bad_rec = copy.deepcopy(r)
          bad_rec['error'] = m
          bad_records.append(bad_rec)
          continue
      if r['barcode'] != '':
        grecs_barcodes[r['barcode']] = i
      grecs_labels[r['label']] = i
      good_records.append(r)
    return good_records, bad_records

  def do_consistency_checks_titer_plate(self, records):
    good_records = []
    bad_records = []
    mandatory_fields = ['rows', 'columns']
    for i, r in enumerate(records):
      reject = 'Rejecting import of line %d.' % i
      if self.missing_fields(mandatory_fields, r):
        m = 'missing mandatory field. '
        self.logger.warning(m + reject)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = m
        bad_records.append(bad_rec)
        continue
      for k in ['rows', 'columns']:
        if not (k in r and (type(r[k]) is int or r[k].isdigit())):
          m = 'undefined/bad value for "%s" column.' % k
          self.logger.warn(m + reject)
          bad_rec = copy.deepcopy(r)
          bad_rec['error'] = m
          bad_records.append(bad_rec)
          continue
      good_records.append(r)
    return good_records, bad_records

  def do_consistency_checks_flow_cell(self, records):
    good_records = []
    bad_records = []
    mandatory_fields = ['number_of_slots']
    for i, r in enumerate(records):
      reject = 'Rejecting import of line %d.' % i
      if self.missing_fields(mandatory_fields, r):
        m = 'missing mandatory field'
        self.logger.warning(m + reject)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = m
        bad_records.append(m)
        continue
      if not('number_of_slots' in r and (type(r['number_of_slots']) is int or
                                         r['number_of_slots'].isdigit())):
        m = 'undefined/bad value for "number_of_slots" column.'
        self.logger.warning(m + reject)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = m
        bad_records.append(bad_rec)
        continue
      good_records.append(r)
    return good_records, bad_records

  def do_consistency_checks_lane(self, records):
    good_records = []
    bad_records = []
    mandatory_fields = ['flow_cell', 'slot']
    for i, r in enumerate(records):
      reject = 'Rejecting import of line %d.' % i
      if self.missing_fields(mandatory_fields, r):
        m = 'missing mandatoty field'
        self.logger.warning(m + reject)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = m
        bad_records.append(m)
        continue
      if not('slot' in r and (type(r['slot']) is int or r['slot'].isdigit())):
        m = 'undefined/bad value for "slot" column.'
        self.logger.warning(m + reject)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = m
        bad_records.append(bad_rec)
        continue
      good_records.append(r)
    return good_records, bad_records

  def process_chunk(self, otsv, chunk, study, asetup, device, category):
    actions = []
    for r in chunk:
      acat = self.kb.ActionCategory.IMPORT
      conf = {
        'setup': asetup,
        'device': device,
        'actionCategory': acat,
        'operator': self.operator,
        'context': study,
        }
      actions.append(self.kb.factory.create(self.kb.Action, conf))
    self.kb.save_array(actions)
    containers = []
    for a, r in it.izip(actions, chunk):
      a.unload()  # we need to do this, or the next save will choke
      conf = {
        'label': r['label'],
        'action': a,
        'status': getattr(ContainerStatus, r['container_status'].upper()),
        }
      for k in 'rows', 'columns', 'slot':
        if k in r and r[k]:
          conf[k] = int(r[k])
      if 'flow_cell' in r and r['flow_cell']:
        conf['flowCell'] = self.preloaded_flowcells[r['flow_cell']]
      if 'number_of_slots' in r and r['number_of_slots'] != '':
        conf['numberOfSlots'] = int(r['number_of_slots'])
      if 'barcode' in r and r['barcode']:
        conf['barcode'] = r['barcode']
      if 'creation_date' in r and r['creation_date'] != '':
        conf['creationDate'] = time.mktime(datetime.strptime(r['creation_date'], '%d/%m/%Y').timetuple())
      containers.append(self.kb.factory.create(self.container_klass, conf))
    self.kb.save_array(containers)
    for c in containers:
      otsv.writerow({
        'study': study.label,
        'label': c.label,
        'type': c.get_ome_table(),
        'vid': c.id,
        })


class RecordCanonizer(core.RecordCanonizer):
  
  def canonize(self, r):
    super(RecordCanonizer, self).canonize(r)
    r.setdefault('container_status', 'INSTOCK')
    r.setdefault('barcode')


help_doc = """
import new Biosample Containers (TiterPlate, FlowCell, Lane) definitions into the KB.
"""


def make_parser(parser):
  def plate_shape(s):
    try:
      rows, cols = tuple(map(int, s.split('x')))
    except ValueError:
      raise ValueError('plate shape must be a pair of integers as RxC')
    return rows, cols
  parser.add_argument('--study', metavar="STRING",
                      help="overrides the study column value")
  parser.add_argument('--container-type', metavar='STRING',
                      choices = Recorder.CONTAINER_TYPE_CHOICES,
                      help='overrides the container_type column value')
  parser.add_argument('--container-status', metavar="STRING",
                      choices = Recorder.STATUS_CHOICES,
                      help="overrides the status column value")
  parser.add_argument('--plate-shape', type=plate_shape, metavar='RxC',
                      help="""plate shape expressed as <rows>x<cols>,
                      e.g., 8x12. Overrides the rows and columns values
                      when importing TiterPlate type containers""")
  parser.add_argument('--number-of-slots', metavar="INT",
                      help="""overrides the number_of_slots column
                      when importing FlowCell type containers""")


def implementation(logger, host, user, passwd, args):
  if args.plate_shape:
    args.rows, args.columns = args.plate_shape
  fields_to_canonize = [
    'study',
    'container_type',
    'container_status',
    ]
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(args.study,
                      host=host, user=user, passwd=passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      keep_tokens=args.keep_tokens, logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  recorder.logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  args.ifile.close()
  if args.container_type == 'TiterPlate':
    fields_to_canonize.extend(['rows', 'columns'])
  elif args.container_type == 'FlowCell':
    fields_to_canonize.append('number_of_slots')
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
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('samples_container', help_doc, make_parser,
                            implementation))
