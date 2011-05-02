"""
Import of TiterPlate
,,,,,,,,,,,,,,,,,,,,

Will read in a csv file with the following columns::

  study  label   barcode rows columns maker model
  ASTUDY p090    2399389 32   48      xxxx  yyy

The maker and model columns are optional

Default plate dimensions can be provided with a flag

  > import -v -i file.csv titer_plate --plate-shape=32x48

"""

from bl.vl.sample.kb import KBError
from core import Core, BadRecord
from version import version

import csv, json
import time, sys

#-----------------------------------------------------------------------------
#FIXME this should be factored out....

import logging, time
logger = logging.getLogger(__name__)
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
               host=None, user=None, passwd=None, keep_tokens=1, operator='Alfred E. Neumann'):
    """
    FIXME

    :param plate_shape: the default titer plate shape
    :type plate_shape: tuple of two positive integers
    """
    super(Recorder, self).__init__(host, user, passwd, keep_tokens)
    self.plate_shape = plate_shape
    #FIXME this can probably go to core....
    self.default_study = None
    if study_label:
      s = self.skb.get_study_by_label(study_label)
      if not s:
        raise ValueError('No known study with label %s' % study_label)
      logger.info('Selecting %s[%d,%s] as default study' % (s.label, s.omero_id, s.id))
      self.default_study = s
    self.known_studies = {}
    self.device = self.get_device('importer-0.0', 'CRS4', 'IMPORT', '0.0')
    self.asetup = self.get_action_setup('importer-version-%s-%s-%f' % (version,
                                                                       "TiterPlate", time.time()),
                                        # FIXME the json below should
                                        # record the app version, and the
                                        # parameters used.  unclear if we
                                        # need to register the file we load
                                        # data from, since it is, most
                                        # likely, a transient object.
                                        json.dumps({'study' : study_label,
                                                    'plate_shape' : plate_shape,
                                                    'operator' : operator,
                                                    'host' : host,
                                                    'user' : user}))
    self.acat  = self.acat_map['IMPORT']
    self.operator = operator
    #
    self.input_rows = {}
    self.counter = 0

  @debug_wrapper
  def record(self, r):
    self.logger.info('processing record[%d] (%s,%s),' % (self.record_counter, r['study'], r['label']))
    self.record_counter += 1
    logger.debug('\tworking on %s' % r)
    try:
      i_study, label, barcode = r['study'], r['label'], r['barcode']
      shape = self.plate_shape if self.plate_shape else tuple(map(int, [r['rows'], r['columns']]))
      #-
      maker = r.get('maker', None)
      model = r.get('model', None)
      #-
      # FIXME: the following is appearing very often...
      study = self.default_study if self.default_study \
              else self.known_studies.setdefault(i_study,
                                                 self.get_study_by_label(i_study))
      #-
      plate = self.get_titer_plate(study, label, barcode, shape, maker, model)
    except KeyError, e:
      self.logger.warn('ignoring record %s because of missing value(%s)' % (r, e))
      return
    # except ValueError, e:
    #   logger.warn('ignoring record %s since %s' % (r, e))
    #   return
    # except (KBError, NotImplementedError), e:
    #   logger.warn('ignoring record %s because it triggers a KB error: %s' % (r, e))
    #   return
    # except Exception, e:
    #   logger.fatal('INTERNAL ERROR WHILE PROCESSING %s (%s)' % (r, e))
    #   sys.exit(1)


  @debug_wrapper
  def create_plate_creation_action(self, study, description=''):
    return self.create_action_helper(self.skb.Action, description,
                                     study, self.device,
                                     self.asetup, self.acat, self.operator)
  @debug_wrapper
  def create_titer_plate(self, study, label, barcode, shape, maker, model,
                         description='import creation'):
    rows, columns = shape
    plate = self.skb.TiterPlate(label=label, barcode=barcode, rows=rows, columns=columns)
    plate.action = self.create_plate_creation_action(study, description=description)
    plate = self.skb.save(plate)
    self.logger.info('created a TiterPlate record (%s,%s)' % (study.label, plate.label))
    return plate

  @debug_wrapper
  def get_titer_plate(self, study, label, barcode, shape, maker, model):
    plate = self.skb.get_titer_plate(barcode=barcode)
    if plate:
      self.logger.info('using (%s,%s) already in kb' % (plate.label, plate.barcode))
      if not plate.label == label:
        msg = 'inconsistent label (>%s< != >%s<) for %s' % (plate.label, label, plate.barcode)
        logger.error(msg)
        raise ValueError(msg)
    elif shape:
      plate = self.create_titer_plate(study, label, barcode, shape, maker, model)
    else:
      raise ValueError('cannot find a plate with barcode <%s>' % barcode)
    return plate

help_doc = """
import new TiterPlate definitions into a virgil system.
"""

def make_parser_titer_plate(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default conxtest study label.
                      It will over-ride the study column value""")
  parser.add_argument('-s', '--plate-shape', type=str, default="32x48",
                      help="""plate shape expressed as <rows>x<cols>, e.g. 32x48 (default value).""")


def import_titer_plate_implementation(args):
  # FIXME it is very likely that the following can be directly
  # implemented as a validation function in the parser definition above.
  try:
    plate_shape = tuple(map(int, args.plate_shape.split('x')))
    if len(plate_shape) != 2:
      raise ValueError('')
  except ValueError, e:
    logger.fatal('illegal value for plate-shape %s' % args.plate_shape)
    sys.exit(1)
  recorder = Recorder(args.study, plate_shape=plate_shape,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  f = csv.DictReader(args.ifile, delimiter='\t')
  for r in f:
    recorder.record(r)

def do_register(registration_list):
  registration_list.append(('titer_plate', help_doc,
                            make_parser_titer_plate,
                            import_titer_plate_implementation))


