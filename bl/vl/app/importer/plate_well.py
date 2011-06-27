"""
Import of PlateWells
,,,,,,,,,,,,,,,,,,,,


Will read in a csv file with the following columns::

  study plate_label label row column bio_sample_label used_volume current_volume
  ASTDY p01         J01   10  1      lab-88           0.1         0.1
  ASTDY p01         J02   10  2      lab-89           0.1         0.1

Each row will be interpreted as follows.
Add a PlateWell to the plate identified by plate_label, The PlateWell
will have, within that plate, the unique identifier label. If row and
column (optional) are provided, it will use that location. If they are
not, it will deduce them from label (e.g., J01 -> row=10,
column=1). Missing labels will be generated as

       '%s%03d' % (chr(row + ord('A') - 1), column)

Badly formed label will bring the rejection of the record. The same
will happen if label, row and column are inconsistent.  The well will
be filled by current_volume material produced by removing used_volume
material taken from the bio material contained in the vessel
identified by bio_sample_label. Row and Column are base 1.

FIXME: currently there is no way to specialize the action performed,
it will always be marked as an ActionCategory.ALIQUOTING.
"""

from bl.vl.sample.kb import KBError
from core import Core, BadRecord
from version import version

import itertools as it
import csv, json
import time, sys
import traceback

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
  into VL.
  """
  def __init__(self, study_label=None,
               used_volume=None,  current_volume=False,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann'):
    """
    FIXME
    """
    super(Recorder, self).__init__(host, user, passwd, keep_tokens)
    self.used_volume    = used_volume
    self.current_volume = current_volume
    self.batch_size = batch_size
    self.operator = operator
    self.device = self.get_device(label='importer.plate_well',
                                  maker='CRS4', model='importer', release='0.1')
    self.asetup = self.get_action_setup('importer.plate_well',
                                        {'used_volume' : used_volume,
                                         'current_volume' : current_volume,
                                         'study_label' : study_label,
                                         'operator' : operator})
    self.known_barcodes = []
    self.known_plates = {}
    self.known_tubes = {}

  #--
  def build_well_full_label(self, clabel, wlabel):
    return ':'.join([clabel, wlabel])
  #--
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
    self.preload_tubes()
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
    plates = self.kb.get_containers(klass=self.kb.TiterPlate)
    for p in plates:
      self.known_plates[p.label] = p
      if hasattr(p, 'barcode') and p.barcode is not None:
        self.known_barcodes.append(p.barcode)
    self.logger.info('there are %d TiterPlate(s) in the kb'
                     % (len(self.known_plates)))

  def preload_labelled_vessels(self, klass, content, preloaded):
    vessels = self.kb.get_vessels(klass, content)
    for v in vessels:
      if hasattr(v, 'label'):
        if isinstance(v, self.kb.PlateWell):
          #FIXME should we check that v.container is an actual TiterPlate?
          plate = v.container
          plate.reload() # Do we really need to do this? We have
                         # already preloaded all Plates...
          label = self.build_well_full_label(plate.label, v.label)
        else:
          label = v.label
        preloaded[label] = v
      if hasattr(v, 'barcode') and v.barcode is not None:
        self.known_barcodes.append(v.barcode)

  def preload_tubes(self):
    self.logger.info('start prefetching vessels')
    self.preload_labelled_vessels(klass=self.kb.Vessel,
                                  content=None,
                                  preloaded=self.known_tubes)
    self.logger.info('there are %d labelled Vessel(s) in the kb'
                     % (len(self.known_tubes)))

  #----------------------------------------------------------------
  def build_well_label(self, r):
    if 'row' in r and 'column' in r:
      row, column = int(r['row']), int(r['column'])
      # row and column are BASE 1 !
      label = '%s%02d' % (chr(row + ord('A') - 1), column)
    else:
      label = r['label']
    return label

  def find_well_coords(self, r):
    if 'row' in r and 'column' in r:
      row, column = int(r['row']), int(r['column'])
    else:
      # FIXME this is ugly, but who cares...
      label = r['label']
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

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    #--
    k_map = {}
    #--
    good_records = []
    for i, r in enumerate(records):
      reject = ' Rejecting import line %d.' % i
      if 'plate_label' not in r or ':' in r['plate_label']:
        f = 'missing/bad plate_label in record.' + reject
        self.logger.error(f)
        continue
      if 'label' not in r and ('row' not in r or 'column' not in r
                               or not r['row'].isdigit()
                               or not r['column'].isdigit()):
        f = 'missing/bad label/row/column in record.' + reject
        self.logger.error(f)
        continue
      label_from_coords = self.build_well_label(r)
      row, column = self.find_well_coords(r)
      if 'label' in r and not r['label'] == label_from_coords:
        f = 'inconsistent label/row/column in record.' + reject
        self.logger.error(f)
        continue
      label = self.build_well_full_label(r['plate_label'], label_from_coords)
      if label in self.known_tubes:
        f = 'there is a pre-existing vessel with label %s.' + reject
        self.logger.warn(f % label)
        continue
      if label in k_map:
        f = ('there is a pre-existing vessel with label %s (in this batch).'
             + reject)
        self.logger.error(f % label)
        continue
      if r['plate_label'] not in self.known_plates:
        f = 'there is no known plate with label %s.' + reject
        self.logger.error(f % r['plate_label'])
        continue
      plate = self.known_plates[r['plate_label']]
      if not 0 < row <= plate.rows:
        f = 'well row is inconsistent with plate[%s].rows.' + reject
        self.logger.error(f % r['plate_label'])
        continue
      if not 0 < column <= plate.columns:
        f = 'well column is inconsistent than plate[%s].columns.' + reject
        self.logger.error(f % r['plate_label'])
        continue
      if r['bio_sample_label'] not in self.known_tubes:
        f = 'there is no known bio_sample with label %s.' + reject
        self.logger.error(f % r['bio_sample_label'])
        continue
      if not r.has_key('current_volume') and not self.current_volume:
        f = 'undefined current_volume for %s.' + reject
        self.logger.error(f % r['label'])
        continue
      if not r.has_key('used_volume') and not self.used_volume:
        f = 'undefined used_volume for %s.' + reject
        self.logger.error(f % r['label'])
        continue
      current_volume = float(r.get('current_volume', self.current_volume))
      used_volume = float(r.get('used_volume', self.used_volume))
      if current_volume > used_volume:
        m = 'current_volume[%s] > used_volume[%s].' + reject
        self.logger.error(m % (r['current_volume'], r['used_volume']))
        continue
      k_map[label] = r
      good_records.append(r)
    self.logger.info('done consistency checks')
    #--
    return good_records

  def process_chunk(self, chunk):
    actions = []
    for r in chunk:
      target = self.known_tubes[r['bio_sample_label']]
      acat = self.kb.ActionCategory.ALIQUOTING
      # FIXME we are not registering details on the amount extracted...
      conf = {'setup' : self.asetup,
              'device': self.device,
              'actionCategory' : acat,
              'operator' : self.operator,
              'context'  : self.default_study,
              'target' : target
              }
      actions.append(self.kb.factory.create(self.kb.ActionOnVessel, conf))
    self.kb.save_array(actions)
    #--
    wells = []
    for a,r in it.izip(actions, chunk):
      # FIXME we need to do this, otherwise the next save will choke.
      a.unload()
      current_volume = float(r.get('current_volume', self.current_volume))
      # FIXME we are not really checking that used_volume is ok.
      # FIXME we are not updating the target current volume.
      used_volume = float(r.get('used_volume', self.used_volume))
      plate = self.known_plates[r['plate_label']]
      well_label = self.build_well_label(r)
      row, column = self.find_well_coords(r)
      slot = (row - 1)* plate.columns + column
      conf = {
        'label'         : r['label'],
        'currentVolume' : current_volume,
        'initialVolume' : current_volume,
        'content'       : self.kb.VesselContent.DNA,
        'status'        : self.kb.VesselStatus.CONTENTUSABLE,
        'action'        : a,
        'container'     : plate,
        'label'         : well_label,
        'slot'          : slot
        }
      wells.append(self.kb.factory.create(self.kb.PlateWell, conf))
    #--
    self.kb.save_array(wells)
    #--
    for w in wells:
      self.logger.info('saved %s[%s] as %s.'
                       % (w.container.label, w.label, w.id))

help_doc = """
import new plate_well definitions into a virgil system.
"""

def make_parser_plate_well(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default conxtest study label.
                      It will over-ride the study column value""")
  parser.add_argument('-V', '--used-volume', type=float,
                      help="""default volume of fluid taken from the source
                      bio_sample. It will over-ride the used_volume
                      column value""")
  parser.add_argument('-C', '--current-volume', type=float,
                      help="""default volume of fluid put in the well.""")

def import_plate_well_implementation(args):
  # FIXME it is very likely that the following can be directly
  # implemented as a validation function in the parser definition above.
  recorder = Recorder(args.study,
                      used_volume=args.used_volume,
                      current_volume=args.current_volume,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  #--
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  recorder.record(records)
  logger.info('done processing file %s' % args.ifile.name)
  #--


def do_register(registration_list):
  registration_list.append(('plate_well', help_doc,
                            make_parser_plate_well,
                            import_plate_well_implementation))


