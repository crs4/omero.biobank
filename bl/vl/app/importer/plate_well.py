"""
Import of PlateWells
,,,,,,,,,,,,,,,,,,,,


Will read in a csv file with the following columns::

  study  label   plate_label row column dna_label volume
  ASTUDY p01.J02 p01         10  2      lab-89 0.1

Default plate dimensions are provided with a flag

  > import -v plate_well -i file.csv --plate-shape=32x48

It will noisily ignore records that do not correspond to a valid plate_label or dna sample.

"""

from bl.vl.sample.kb import KBError
from core import Core, BadRecord
from version import version

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
  into VL.
  """
  def __init__(self, study_label=None, volume=None,  update_volume=False,
               host=None, user=None, passwd=None, keep_tokens=1, operator='Alfred E. Neumann'):
    """
    FIXME
    """
    super(Recorder, self).__init__(host, user, passwd, keep_tokens)
    self.volume = float(volume) if volume else volume
    self.update_volume = update_volume
    #FIXME this can probably go to core....
    self.default_study = None
    if study_label:
      s = self.skb.get_study_by_label(study_label)
      if not s:
        raise ValueError('No known study with label %s' % study_label)
      self.logger.info('Selecting %s[%d,%s] as default study' % (s.label, s.omero_id, s.id))
      self.default_study = s
    self.known_studies = {}
    self.device = self.get_device('importer-0.0', 'CRS4', 'IMPORT', '0.0')
    self.asetup = self.get_action_setup('importer-version-%s-%s-%f' % (version, "PlateWell", time.time()),
                                        # FIXME the json below should
                                        # record the app version, and the
                                        # parameters used.  unclear if we
                                        # need to register the file we load
                                        # data from, since it is, most
                                        # likely, a transient object.
                                        json.dumps({'study' : study_label,
                                                    'volume' : volume,
                                                    'update_volume' : update_volume,
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
    self.logger.debug('\tworking on %s' % r)
    try:
      i_study, label, plate_label, dna_label = \
               r['study'], r['label'], r['plate_label'], r['dna_label']
      row, column  = map(int, [r['row'], r['column']])
      delta_volume = self.volume if self.volume else float(r['volume'])
      #-
      study = self.default_study if self.default_study \
              else self.known_studies.setdefault(i_study,
                                                 self.get_study_by_label(i_study))
      plate = self.get_titer_plate(study=study, label=plate_label)
      pw = self.skb.get_well_of_plate(plate=plate, row=row, column=column)
      if pw:
        self.logger.info('not loading PlateWell[%s, %s]. Is already in KB.' % (i_study, label))
        return
      dna_sample = self.get_dna_sample(label=dna_label)
      if self.update_volume:
        current_volume = dna_sample.current_volume
        if current_volume < delta_volume:
          raise ValueError('dna_sample.current_volume(%f) < requested plate_well volume(%f)' % \
                           (current_volume, delta_volume))
        self.create_plate_well(study=study,
                               container=plate, sample=dna_sample,
                               label=label,
                               row=row, column=column,
                               volume=delta_volume, description=json.dumps(r))
        self.logger.info('saved plate_well (%s,%s),' % (study.label, label))
        old_current_volume = dna_sample.current_volume
        current_volume -= delta_volume
        dna_sample.current_volume = current_volume
        self.skb.save(dna_sample)
        self.logger.info('updated volume of dna_sample %s from %f to %f' % (dna_sample.label,
                                                                            old_current_volume,
                                                                            current_volume))

      else:
        self.create_plate_well(study=study,
                               container=plate, sample=dna_sample,
                               label=label, row=row, column=column,
                               volume=delta_volume, description=json.dumps(r))
        self.logger.info('saved plate_well (%s,%s),' % (study.label, label))
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
  def create_plate_well(self, study, container, sample,
                        label, row, column, volume, description=''):
    action = self.create_action_on_sample(study, sample,
                                          description=description)
    plate_well = self.skb.PlateWell(sample=sample, container=container,
                                    row=row, column=column,
                                    volume=volume)
    plate_well.label  = label
    plate_well.action = action
    plate_well.outcome = self.outcome_map['OK']
    return self.skb.save(plate_well)

  @debug_wrapper
  def create_action_on_sample(self, study, sample, description=''):
    return self.create_action_helper(self.skb.ActionOnSample, description,
                                     study, self.device,
                                     self.asetup, self.acat, self.operator,
                                     sample)


  @debug_wrapper
  def get_titer_plate(self, study, label):
    # currently study is ignored.
    titer_plate = self.skb.get_titer_plate(label=label)
    if not titer_plate:
      raise ValueError('cannot find a plate with label <%s>' % label)
    return titer_plate

  @debug_wrapper
  def get_dna_sample(self, label):
    dna_sample = self.skb.get_dna_sample(label=label)
    if not dna_sample:
      raise ValueError('cannot find a dna sample with label <%s>' % label)
    return dna_sample

help_doc = """
import new plate_well definitions into a virgil system.
"""

def make_parser_plate_well(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default conxtest study label.
                      It will over-ride the study column value""")
  parser.add_argument('-V', '--volume', type=float,
                      help="""default volume of fluid assigned to a plate well.
                      It will over-ride the volume column value""")
  parser.add_argument('--update-volume', action='store_true', default=False,
                      help="""if set, it will subract the amount required by the plate well row from the
                              referenced dna sample vial""")

def import_plate_well_implementation(args):
  # FIXME it is very likely that the following can be directly
  # implemented as a validation function in the parser definition above.
  recorder = Recorder(args.study, volume=args.volume, update_volume=args.update_volume,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  f = csv.DictReader(args.ifile, delimiter='\t')
  for r in f:
    recorder.record(r)

def do_register(registration_list):
  registration_list.append(('plate_well', help_doc,
                            make_parser_plate_well,
                            import_plate_well_implementation))


