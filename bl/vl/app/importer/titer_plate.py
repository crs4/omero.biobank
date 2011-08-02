"""
Import of TiterPlate
,,,,,,,,,,,,,,,,,,,,

Will read in a csv file with the following columns::

  study  label   barcode rows columns maker model
  ASTUDY p090    2399389 32   48      xxxx  yyy

The maker and model columns are optional, as well as the barcode one.
Default plate dimensions  can be provided with a flag

.. code-block:: bash
   ${IMPORT} ${SERVER_OPTS} -i titer_plates.tsv
                            -o titer_plates_mapping.tsv\
                            titer_plate\
                            --study  ${DEFAULT_STUDY}\
                            --plate-shape=32x48\
                            --maker=foomaker\
                            --model=foomodel
                            --plate-status=INSTOCK

"""

from core import Core, BadRecord
from version import version

from bl.vl.kb.drivers.omero.objects_collections import ContainerStatus

import itertools as it
import csv, json
import time, sys



class Recorder(Core):
  """
  An utility class that handles the actual recording of PlateWell(s)
  into VL, including TiterPlate(s) generation as needed.
  """
  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann',
               action_setup_conf=None,
               logger=None):
    """
    FIXME

    """
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   study_label=study_label,
                                   logger=logger)
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
    #--
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
    self.logger.info('there are %d TiterPlate(s) in the kb'
                     % (len(self.known_plates)))

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    #--
    good_records = []
    for i, r in enumerate(records):
      reject = 'Rejecting import of line %d.' % i
      if r['barcode'] in self.known_barcodes:
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
    #--
    return good_records

  def process_chunk(self, otsv, chunk, study, asetup, device, category):
    actions = []
    for r in chunk:
      acat = self.kb.ActionCategory.IMPORT
      # FIXME we are not registering details on the amount extracted...
      conf = {'setup' : asetup,
              'device': device,
              'actionCategory' : acat,
              'operator' : self.operator,
              'context'  : study
              }
      actions.append(self.kb.factory.create(self.kb.Action, conf))
    self.kb.save_array(actions)
    #--
    titer_plates = []
    for a,r in it.izip(actions, chunk):
      # FIXME we need to do this, otherwise the next save will choke.
      a.unload()
      conf = {
        'label'   : r['label'],
        'rows'    : int(r['rows']),
        'columns' : int(r['columns']),
        'action'  : a,
        'status'  : getattr(ContainerStatus,
                            r['plate_status'].upper()),
        }
      for k in ['barcode', 'maker', 'model']:
        if k in r:
          conf[k] = r[k]
      titer_plates.append(self.kb.factory.create(self.kb.TiterPlate, conf))
    #--
    self.kb.save_array(titer_plates)
    for p in titer_plates:
      otsv.writerow({'study' : study.label,
                     'label' : p.label,
                     'type'  : p.get_ome_table(),
                     'vid'   : p.id })


def canonize_records(args, records):
  fields = ['study', 'maker', 'model', 'rows', 'columns',
            'plate_status']
  for f in fields:
    if hasattr(args, f) and getattr(args,f) is not None:
      for r in records:
        r[f] = getattr(args, f)

  # handle special cases
  if args.plate_shape:
    setattr(args, 'rows',   args.plate_shape[0])
    setattr(args, 'coumns', args.plate_shape[1])

  for r in records:
    if not 'plate_status' in r:
      r['plate_status'] = 'INSTOCK'


help_doc = """
import new TiterPlate definitions into a virgil system.
"""

def make_parser_titer_plate(parser):
  def plate_shape(s):
    shape = tuple(map(int, s.split('x')))
    if len(shape) != 2:
      raise ValueError('')
    return shape

  parser.add_argument('--study', type=str,
                      help="""default conxtest study label.
                      It will over-ride the study column value""")
  parser.add_argument('--plate-shape', type=plate_shape,
                      help="""plate shape expressed as <rows>x<cols>,
                      e.g. 8x12.  It will override
                      the rows and columns cols""")
  parser.add_argument('--maker', type=str,
                      help="""the plate maker,
                      it will override the corresponding column""")
  parser.add_argument('--model', type=str,
                      help="""the plate model,
                      it will override the corresponding column""")
  parser.add_argument('--plate-status', type=str,
                      choices=[x.enum_label()
                               for x in ContainerStatus.__enums__],
                      help="""default plate status.  It will
                      over-ride the plate_status column value, if any.
                      """)


def import_titer_plate_implementation(logger, args):
  #--
  action_setup_conf = {}
  for x in dir(args):
    if not (x.startswith('_') or x.startswith('func')):
      action_setup_conf[x] = getattr(args, x)
  #FIXME HACKS
  action_setup_conf['ifile'] = action_setup_conf['ifile'].name
  action_setup_conf['ofile'] = action_setup_conf['ofile'].name
  #---
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      keep_tokens=args.keep_tokens,
                      logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  #--
  canonize_records(args, records)
  #--
  o = csv.DictWriter(args.ofile,
                     fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t')
  o.writeheader()
  recorder.record(records, o)
  #--
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('titer_plate', help_doc,
                            make_parser_titer_plate,
                            import_titer_plate_implementation))


