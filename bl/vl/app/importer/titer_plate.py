# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import titerplate
=================

A full TiterPlate record will have the following columns::

  study  label   barcode rows columns plate_status maker model
  ASTUDY p090    2399389 32   48      xxxx  yyy

The maker and model columns are optional, as well as the barcode one.
Default plate dimensions can be provided via command line.
"""

import os, csv, json
import itertools as it

from bl.vl.kb.drivers.omero.objects_collections import ContainerStatus

import core
from version import version


PLATE_STATUS_CHOICES = [x.enum_label() for x in ContainerStatus.__enums__]


class Recorder(core.Core):

  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann',
               action_setup_conf=None, logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.batch_size = batch_size
    self.operator = operator
    self.action_setup_conf = action_setup_conf

  def record(self, records, otsv):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if not records:
      self.logger.warn('no records')
      return
    self.preload_plates()
    records = self.do_consistency_checks(records)
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

  def preload_plates(self):
    self.logger.info('start prefetching plates')
    self.known_plates = {}
    self.known_barcodes = []
    plates = self.kb.get_containers(klass=self.kb.TiterPlate)
    for p in plates:
      self.known_plates[p.label] = p
      if hasattr(p, 'barcode') and p.barcode is not None:
        self.known_barcodes.append(p.barcode)
    self.logger.info('there are %d TiterPlate(s) in the kb' %
                     (len(self.known_plates)))

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    good_records = []
    for i, r in enumerate(records):
      reject = 'Rejecting import of line %d.' % i
      if r['barcode'] and r['barcode'] in self.known_barcodes:
        m = 'there is a pre-existing object with barcode %s. ' + reject
        self.logger.warn(m % r['barcode'])
        continue
      if self.known_plates.has_key(r['label']):
        f = 'there is a pre-existing plate with label %s. ' + reject
        self.logger.warn(f % r['label'])
        continue
      for k in ['rows', 'columns']:
        if not (k in r
                and (type(r[k]) is int or r[k].isdigit())):
          msg = 'undefined/bad value for % for %s. ' + reject
          self.logger.error(msg % (k, r['label']))
          continue
      good_records.append(r)
    self.logger.info('done consistency checks')
    return good_records

  def process_chunk(self, otsv, chunk, study, asetup, device, category):
    actions = []
    for r in chunk:
      acat = self.kb.ActionCategory.IMPORT
      # TODO: register details on the amount extracted
      conf = {
        'setup': asetup,
        'device': device,
        'actionCategory': acat,
        'operator': self.operator,
        'context': study,
        }
      actions.append(self.kb.factory.create(self.kb.Action, conf))
    self.kb.save_array(actions)
    titer_plates = []
    for a, r in it.izip(actions, chunk):
      a.unload()  # we need to do this, or the next save will choke
      conf = {
        'label': r['label'],
        'rows': int(r['rows']),
        'columns': int(r['columns']),
        'action': a,
        'status': getattr(ContainerStatus, r['plate_status'].upper()),
        }
      for k in 'barcode', 'maker', 'model':
        if r[k]:
          conf[k] = r[k]
      titer_plates.append(self.kb.factory.create(self.kb.TiterPlate, conf))
    self.kb.save_array(titer_plates)
    for p in titer_plates:
      otsv.writerow({
        'study': study.label,
        'label': p.label,
        'type': p.get_ome_table(),
        'vid': p.id,
        })


class RecordCanonizer(core.RecordCanonizer):
  
  def canonize(self, r):
    super(RecordCanonizer, self).canonize(r)
    r.setdefault('plate_status', 'INSTOCK')
    for k in 'barcode', 'maker', 'model':
      r.setdefault(k)


help_doc = """
import new TiterPlate definitions into the KB.
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
  parser.add_argument('--plate-shape', type=plate_shape, metavar="RxC",
                      help="""plate shape expressed as <rows>x<cols>,
                      e.g., 8x12. Overrides the rows and columns values""")
  parser.add_argument('--maker', metavar="STRING",
                      help="overrides the maker column value")
  parser.add_argument('--model', metavar="STRING",
                      help="overrides the model column value")
  parser.add_argument('--plate-status', metavar="STRING",
                      choices=PLATE_STATUS_CHOICES,
                      help="overrides the plate_status column value")


def implementation(logger, host, user, passwd, args):
  if args.plate_shape:
    args.rows, args.columns = args.plate_shape
  fields_to_canonize = [
    'study',
    'maker',
    'model',
    'rows',
    'columns',
    'plate_status',
    ]
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(args.study,
                      host=host, user=user, passwd=passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      keep_tokens=args.keep_tokens, logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
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
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('titer_plate', help_doc, make_parser,
                            implementation))
