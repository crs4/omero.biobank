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
                            --container-type Tube \
                            --container-content BLOOD \
                            --container-status  USABLE \
                            --current-volume 20

where container-content is taken from the enum VesselContent possible
values and container-status from the enum VesselStatus

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
                            --container-type Tube \
                            --source-type Tube \
                            --container-content DNA \
                            --container-status  USABLE

"""

from core import Core, BadRecord

from version import version

import csv, json, time

import itertools as it

# FIXME this is an hack...
from bl.vl.kb.drivers.omero.vessels import VesselContent, VesselStatus

class BioSampleRecorder(Core):
  """
  A class that helps in the recording of BioSamples subclasses into VL
  """
  # FIXME: the klass_name thing is a kludge...
  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               operator='Alfred E. Neumann', batch_size=10000,
               action_setup_conf=None, logger=None):
    super(BioSampleRecorder, self).__init__(host, user, passwd, keep_tokens, logger=logger)
    self.operator = operator
    self.batch_size = batch_size
    self.action_setup_conf = action_setup_conf
    self.preloaded_sources = {}
    self.preloaded_containers = {}


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
    study        = self.find_study(records)
    self.source_klass = self.find_source_klass(records)
    self.container_klass = self.find_container_klass(records)
    self.preload_sources(self.source_klass)
    self.preload_containers(self.container_klass)
    #--
    records = self.do_consistency_checks(records)
    #--
    device = self.get_device('importer-%s' % version,
                             'CRS4', 'IMPORT', version)
    asetup = self.get_action_setup('import-prog-%f' % time.time(),
                                   json.dumps(self.action_setup_conf))
    acat  = self.kb.ActionCategory.IMPORT
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(otsv, c, study, asetup, device, acat)
      self.logger.info('done processing chunk %d' % i)


  def find_study(self, records):
    study_label = records[0]['study']
    for r in records:
      if r['study'] != study_label:
        m = 'all records should have the same study label'
        self.logger.critical(m)
        raise ValueError(m)
    return self.get_study(study_label)

  def find_klass(self, col_name, records):
    o_type = records[0][col_name]
    for r in records:
      if r[col_name] != o_type:
        m = 'all records should have the same %s' % col_name
        self.logger.critical(m)
        raise ValueError(m)
    return getattr(self.kb, o_type)

  def find_source_klass(self, records):
    return self.find_klass('source_type', records)

  def find_container_klass(self, records):
    return self.find_klass('container_type', records)

  def preload_sources(self, klass):
    self.logger.info('start preloading sources')
    sources = self.kb.get_objects(klass)
    for s in sources:
      assert not s.id in self.preloaded_sources
      self.preloaded_sources[s.id] = s
    self.logger.info('done preloading sources')

  def preload_containers(self, klass):
    self.logger.info('start preloading containers')
    containers = self.kb.get_objects(klass)
    for c in containers:
      assert not c.id in self.preloaded_containers
      self.preloaded_containers[c.label] = c
    self.logger.info('done preloading containers')

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
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d.' % i
      # if self.known_barcodes.has_key(r['barcode']):
      # m = ('there is a pre-existing object with barcode %s. '
      #       + 'Rejecting import.')
      #   self.logger.warn(m % r['barcode'])
      #   continue
      if r['label'] in self.preloaded_containers:
        f = 'there is a pre-existing container with label %s. ' + reject
        self.logger.warn(f % r['label'])
        continue
      if not r['source'] in  self.preloaded_sources:
        f = 'there is no known source for %s. ' + reject
        self.logger.warn(f % r['source'])
        continue
      if not r.has_key('current_volume'):
        f = 'undefined current_volume for %s. ' + reject
        self.logger.warn(f % r['label'])
        continue
      good_records.append(r)
    self.logger.info('done consistency checks')
    #--
    return good_records

  def process_chunk(self, otsv, chunk,
                    study, asetup, device, category):
    aklass = {self.kb.Individual : self.kb.ActionOnIndividual,
              self.kb.Tube       : self.kb.ActionOnVessel}
    actions = []
    for r in chunk:
      target = self.preloaded_sources[r['source']]
      conf = {'setup' : asetup,
              'device': device,
              'actionCategory' : category,
              'operator' : self.operator,
              'context'  : study,
              'target' : target
              }
      actions.append(self.kb.factory.create(aklass[target.__class__], conf))
    assert len(actions) == len(chunk)
    self.kb.save_array(actions)
    #--
    vessels = []
    for a,r in it.izip(actions, chunk):
      a.unload()
      current_volume = float(r['current_volume'])
      initial_volume = current_volume
      conf = {
        'label'         : r['label'],
        'currentVolume' : current_volume,
        'initialVolume' : initial_volume,
        'content' : getattr(self.kb.VesselContent,
                            r['container_content'].upper()),
        'status'  : getattr(self.kb.VesselStatus,
                            r['container_status'].upper()),
        'action'        : a,
        }
      if r.has_key('barcode'):
        conf['barcode'] = r['barcode']
      vessels.append(self.kb.factory.create(self.container_klass, conf))
    #--
    assert len(vessels) == len(chunk)
    self.kb.save_array(vessels)
    for v in vessels:
      otsv.writerow({'study' : study.label,
                     'label' : v.label,
                     'type'  : v.get_ome_table(),
                     'vid'   : v.id })

def canonize_records(args, records):
  fields = ['study', 'source_type',
            'container_type', 'container_content', 'container_status',
            'current_volume', 'used_volume']
  for f in fields:
    if hasattr(args, f) and getattr(args,f) is not None:
      for r in records:
        r[f] = getattr(args, f)

def make_parser_biosample(parser):
  parser.add_argument('--study', type=str,
                      help="""default study assumed as context for the
                      import action.  It will
                      over-ride the study column value, if any.""")
  parser.add_argument('--container-type', type=str,
                      choices=['Tube'],
                      help="""default container type.  It will
                      over-ride the study column value.
                      """)
  parser.add_argument('--source-type', type=str,
                      choices=['Tube', 'Individual'],
                      help="""default source type.  It will
                      over-ride the source_type column value.
                      """)
  parser.add_argument('--container-content', type=str,
                      choices=[x.enum_label() for x in VesselContent.__enums__],
                      help="""default container content.  It will
                      over-ride the container_column value, if any.
                      """)
  parser.add_argument('--container-status', type=str,
                      choices=[x.enum_label() for x in VesselStatus.__enums__],
                      help="""default container status.  It will
                      over-ride the container_status column value, if any.
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
  #--
  action_setup_conf = {}
  for x in dir(args):
    if not (x.startswith('_') or x.startswith('func')):
      action_setup_conf[x] = getattr(args, x)
  #FIXME HACKS
  action_setup_conf['ifile'] = action_setup_conf['ifile'].name
  action_setup_conf['ofile'] = action_setup_conf['ofile'].name
  #---
  recorder = BioSampleRecorder(host=args.host, user=args.user,
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


