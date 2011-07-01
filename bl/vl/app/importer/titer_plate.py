"""
Import of TiterPlate
,,,,,,,,,,,,,,,,,,,,

Will read in a csv file with the following columns::

  study  label   barcode rows columns maker model
  ASTUDY p090    2399389 32   48      xxxx  yyy

The maker and model columns are optional, as well as the barcode one.

Default plate dimensions  can be provided with a flag

  > import -v -i file.csv titer_plate --plate-shape=32x48

"""

from bl.vl.kb import KBError
from core import Core, BadRecord
from version import version

import itertools as it
import csv, json
import time, sys

#-----------------------------------------------------------------------------
#FIXME this should be factored out....

import logging, time
logger = logging.getLogger()
counter = 0
def debug_wrapper(f):
  def debug_wrapper_wrapper(*args, **kv):
    global counter
    now = time.time()
    counter += 1
    logger.debug('%s[%d] in' % (f.__name__, counter))
    res = f(*args, **kv)
    logger.debug('%s[%d] out (%f)' % (f.__name__, counter, time.time() - now))
    counter -= 1
    return res
  return debug_wrapper_wrapper
#-----------------------------------------------------------------------------

class Recorder(Core):
  """
  An utility class that handles the actual recording of PlateWell(s)
  into VL, including TiterPlate(s) generation as needed.
  """
  def __init__(self, study_label=None,
               plate_shape=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann'):
    """
    FIXME

    :param plate_shape: the default titer plate shape
    :type plate_shape: tuple of two positive integers
    """
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   study_label)
    self.plate_shape = plate_shape
    self.batch_size = batch_size
    self.operator = operator
    self.device = self.get_device(label='importer.titer_plate',
                                  maker='CRS4', model='importer', release='0.1')
    self.asetup = self.get_action_setup('importer.dna_sample',
                                        {'study_label' : study_label,
                                         'default_plate_shape': plate_shape,
                                         'operator' : operator})

  def record(self, records):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    #--
    if not records:
      self.logger.warn('no records')
      return
    self.choose_relevant_study(records)
    self.preload_plates()
    #--
    records = self.do_consistency_checks(records)
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(c)
      self.logger.info('done processing chunk %d' % i)

  def choose_relevant_study(self, records):
    if self.default_study:
      return
    study_label = records[0]['study']
    for r in records:
      if r['study'] != study_label:
        m = 'all records should have the same study label'
        self.logger.critical(m)
        raise ValueError(m)
    self.default_study = self.get_study(study_label)

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
    reject = 'Rejecting import.'
    for r in records:
      if r['barcode'] in self.known_barcodes:
        m = ('there is a pre-existing object with barcode %s. '
            + 'Rejecting import.')
        self.logger.warn(m % r['barcode'])
        continue
      if self.known_plates.has_key(r['label']):
        f = 'there is a pre-existing plate with label %s. ' + reject
        self.logger.warn(f % r['label'])
        continue
      if not 'rows' in r or not r['rows'].isdigit():
        f = 'undefined/bad value for rows for %s. ' + reject
        self.logger.error(f % r['label'])
        continue
      if not 'columns' in r or not r['columns'].isdigit():
        f = 'undefined/bad value columns for %s. ' + reject
        self.logger.error(f % r['label'])
        continue
      good_records.append(r)
    self.logger.info('done consistency checks')
    #--
    return good_records

  def process_chunk(self, chunk):
    actions = []
    for r in chunk:
      acat = self.kb.ActionCategory.IMPORT
      # FIXME we are not registering details on the amount extracted...
      conf = {'setup' : self.asetup,
              'device': self.device,
              'actionCategory' : acat,
              'operator' : self.operator,
              'context'  : self.default_study
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
        }
      for k in ['barcode', 'maker', 'model']:
        if k in r:
          conf[k] = r[k]
      titer_plates.append(self.kb.factory.create(self.kb.TiterPlate, conf))
    #--
    self.kb.save_array(titer_plates)
    for p in titer_plates:
      self.logger.info('saved %s[%sx%s] as %s.'
                       % (p.label, p.rows, p.columns, p.id))

help_doc = """
import new TiterPlate definitions into a virgil system.
"""

def make_parser_titer_plate(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default conxtest study label.
                      It will over-ride the study column value""")
  # FIXME do we really need this flag?
  parser.add_argument('-s', '--plate-shape', type=str,
                      help="""plate shape expressed as <rows>x<cols>, e.g. 8x12""")


def import_titer_plate_implementation(args):
  # FIXME it is very likely that the following can be directly
  # implemented as a validation function in the parser definition above.
  if args.plate_shape:
    try:
      plate_shape = tuple(map(int, args.plate_shape.split('x')))
      if len(plate_shape) != 2:
        raise ValueError('')
    except ValueError, e:
      logger.fatal('illegal value for plate-shape %s' % args.plate_shape)
      sys.exit(1)
  else:
    plate_shape = None
  recorder = Recorder(args.study, plate_shape=plate_shape,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  recorder.record(records)
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('titer_plate', help_doc,
                            make_parser_titer_plate,
                            import_titer_plate_implementation))


