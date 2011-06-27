"""
Import of dna samples
=======================


Will read in a tsv file with the following columns::

  study label barcode   bio_sample_label used_volume current_volume status
  xxx   dna01 328989238 id2              40             20             USABLE
  xxx   dna03 328989228 id3              40             20             USABLE
  ....

Where the label is the label on the vessel containing the dna_sample,
barcode is its (optional) barcode, bio_sample_label is the label of
the vessel that contained the biological sample from which the dna was
either extracted or siphoned out.

FIXME: does the following make sense?

In default, if the content of the object indicated by the
bio_sample_label is VesselContent.DNA the connecting action will be
marked as ActionCategory.ALIQUOTING. Otherwise, the the connecting
action will be marked as ActionCategory.EXTRACTION.

The column `used_volume` indicates the amount, in volume (ml), of the
bio sample that was used to generate the `current_volume` (ml) of the
new dna sample.

Consistency checks will be performed on the used/transferred volume, and
the source vessel volume will be updated. [FIXME, not implemented yet]

Status should be one of the values listed in the enum
ome.model.vl.VesselStatus. FIXME add a link

Records that point to an unknown (bio_sample_label)
pair will be noisily ignored. The same will happen to records that
have the same label or barcode of a previously seen dna sample.

Study defines the context in which the import occurred.

"""
from core import Core, BadRecord

import csv, json

import itertools as it

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
  def __init__(self, study_label=None,
               used_volume=None, current_volume=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann'):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   study_label)
    self.used_volume    = used_volume
    self.current_volume = current_volume
    self.batch_size = batch_size
    self.operator = operator
    self.device = self.get_device(label='importer.dna_sample',
                                  maker='CRS4', model='importer', release='0.1')
    self.asetup = self.get_action_setup('importer.dna_sample',
                                        {'used_volume' : used_volume,
                                         'current_volume' : current_volume,
                                         'study_label' : study_label,
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

  def preload_labelled_vessels(self, klass, content, preloaded):
    vessels = self.kb.get_vessels(klass, content)
    for v in vessels:
      if hasattr(v, 'label'):
        preloaded[v.label] = v

  def preload_tubes(self):
    self.logger.info('start prefetching vessels')
    self.known_tubes = {}
    self.preload_labelled_vessels(klass=self.kb.Vessel,
                                  content=None,
                                  preloaded=self.known_tubes)
    self.logger.info('there are %d labelled Vessel(s) in the kb'
                     % (len(self.known_tubes)))

  #----------------------------------------------------------------

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    #--
    k_map = {}
    for r in records:
      if r['label'] in k_map:
        self.logger.error('multiple record for the same label: %s. Rejecting.'
                          % r['label'])
      else:
        k_map[r['label']] = r
    records = k_map.values()
    #--
    good_records = []
    reject = 'Rejecting import.'
    for r in records:
      # if self.known_barcodes.has_key(r['barcode']):
      # m = ('there is a pre-existing object with barcode %s. '
      #       + 'Rejecting import.')
      #   self.logger.warn(m % r['barcode'])
      #   continue
      if self.known_tubes.has_key(r['label']):
        f = 'there is a pre-existing tube with label %s. ' + reject
        self.logger.warn(f % r['label'])
        continue
      if not self.known_tubes.has_key(r['bio_sample_label']):
        f = 'there is no known bio_sample with label %s. ' + reject
        self.logger.warn(f % r['bio_sample_label'])
        continue
      if not r.has_key('current_volume') and not self.current_volume:
        f = 'undefined current_volume for %s. ' + reject
        self.logger.warn(f % r['label'])
        continue
      if not r.has_key('used_volume') and not self.used_volume:
        f = 'undefined used_volume for %s. ' + reject
        self.logger.warn(f % r['label'])
        continue
      current_volume = float(r.get('current_volume', self.current_volume))
      used_volume = float(r.get('used_volume', self.used_volume))
      if current_volume > used_volume:
        m = '(%s) current_volume[%s] > used_volume[%s]. ' + reject
        self.logger.warn(m % (r['label'],
                              r['current_volume'], r['used_volume']))
        continue
      good_records.append(r)
    self.logger.info('done consistency checks')
    #--
    return good_records

  def process_chunk(self, chunk):
    actions = []
    for r in chunk:
      target = self.known_tubes[r['bio_sample_label']]
      if target.content == self.kb.VesselContent.DNA:
        acat = self.kb.ActionCategory.ALIQUOTING
      else:
        acat = self.kb.ActionCategory.EXTRACTION
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
    vessels = []
    for a,r in it.izip(actions, chunk):
      # FIXME we need to do this, otherwise the next save will choke.
      a.unload()
      current_volume = float(r.get('current_volume', self.current_volume))
      # FIXME we are not really checking that used_volume is ok.
      # FIXME we are not updating the target current volume.
      used_volume = float(r.get('used_volume', self.used_volume))
      conf = {
        'label'         : r['label'],
        'currentVolume' : current_volume,
        'initialVolume' : current_volume,
        'content'       : self.kb.VesselContent.DNA,
        'status'        : self.kb.VesselStatus.CONTENTUSABLE,
        'action'        : a,
        }
      if r.has_key('barcode'):
        conf['barcode'] = r['barcode']
      vessels.append(self.kb.factory.create(self.kb.Tube, conf))
    #--
    self.kb.save_array(vessels)
    for v in vessels:
      self.logger.info('saved %s as %s.' % (v.label, v.id))

def make_parser_dna_sample(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""default study used as context
                      for the import action.  It will
                      over-ride the study column value.""")
  parser.add_argument('--used-volume', type=float,
                      help="""default amount of source volume used to generate
                      the dna sample.  It will over-ride the used_volume
                      column value.""")
  parser.add_argument('--current-volume', type=float,
                      help="""default current volume assigned to
                      the dna sample.
                      It will over-ride the current_volume column value.""")
  parser.add_argument('-N', '--batch-size', type=int,
                      help="""Size of the batch of individuals
                      to be processed in parallel (if possible)""",
                      default=1000)

def import_dna_sample_implementation(args):
  if args.used_volume:
    logger.warn('FIXME used-volume flag is currently disabled.')
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

help_doc = """
import new dna sample definitions into a virgil system and attach
them to previously registered bio samples.
"""

def do_register(registration_list):
  registration_list.append(('dna_sample', help_doc,
                            make_parser_dna_sample,
                            import_dna_sample_implementation))


