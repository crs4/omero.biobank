"""
Import of Device(s)
===================

Will read in a tsv file with the following columns::

  study  device_type    label   barcode maker model release location
  BSTUDY Scanner pula01  8989898 Affymetrix  GeneChip Scanner 3000 7G  Pula bld. 5
  BSTUDY Chip    chip001 8329482 Affymetrix  Genome-Wide Human SNP Array 6.0  None

All devices have a type, a label, an optional barcode, a maker, a
model and a release, and, possibly a physical location. In the example
above, in the first line we have defined a scanner, which is
physically in the lab in Pula, building 5.  The following line defines
a chip. A special case

.. code-block:: bash

  bash> cat devices.tsv
  device_type label barcode maker model release location
  Scanner pula01  8989898 Affymetrix  GeneChip Scanner 3000 7G  Pula Ed.5
  Chip  chip001 8329482 Affymetrix  Genome-Wide Human SNP Array 6.0 None
  Chip  chip002 8329483 Affymetrix  Genome-Wide Human SNP Array 6.0 None
  Chip  chip003 8329484 Affymetrix  Genome-Wide Human SNP Array 6.0 None
  Chip  chip004 8329485 Affymetrix  Genome-Wide Human SNP Array 6.0 None
  Chip  chip005 8329486 Affymetrix  Genome-Wide Human SNP Array 6.0 None
  Chip  chip006 8329487 Affymetrix  Genome-Wide Human SNP Array 6.0 None
  bash> ${IMPORTER} -i devices.tsv -o devices_mapping.tsv device --study BSTUDY
  bash> cat devices_mapping.tsv
  study label type  vid
  BSTUDY  pula01  Scanner V0BAF6C3F471284CBF9A867A62B26E688A
  BSTUDY  chip001 Chip  V03C9FB0C268594D7BBBB359BBF09163E4
  BSTUDY  chip002 Chip  V03C658EE6C1E8494FB239E791EA0BA061
  BSTUDY  chip003 Chip  V0E37789C133784DB9B9FF6EA81A3B1024
  BSTUDY  chip004 Chip  V0ABA7C0D1D90F46F7B36F1FB8ED626AEF
  BSTUDY  chip005 Chip  V02CA380A20926473386A7A15A5270B939
  BSTUDY  chip006 Chip  V065049E68E56946E09CE51E448756F01F

"""

from core import Core, BadRecord
from version import version

import itertools as it
import csv, json
import time, sys

class Recorder(Core):
  """
  An utility class that handles the actual recording of Device(s)
  into VL, including Device(s) generation as needed.
  """
  def __init__(self,
               study_label,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size
               =1000, operator='Alfred E. Neumann',
               action_setup_conf=None,
               logger=None):
    """
    FIXME
    """
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.batch_size = batch_size
    self.operator = operator
    self.action_setup_conf = action_setup_conf
    self.preloaded_devices = {}
    self.preloaded_markers_sets = {}
    self.known_barcodes = []

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
    study  = self.find_study(records)
    self.preload_devices()
    self.preload_markers_sets()

    records = self.do_consistency_checks(records)

    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(c, otsv, study)
      self.logger.info('done processing chunk %d' % i)

  def preload_devices(self):
    self.logger.info('start preloadind devices')
    devices = self.kb.get_objects(self.kb.Device)
    for d in devices:
      self.preloaded_devices[d.label] = d
      if hasattr(d, 'barcode') and d.barcode is not None:
        self.known_barcodes.append(d.barcode)
    self.logger.info('there are %d Device(s) in the kb'
                     % (len(self.preloaded_devices)))

  def preload_markers_sets(self):
    self.preload_by_type('markers_sets', self.kb.SNPMarkersSet,
                         self.preloaded_markers_sets)

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    k_map = {}
    #--
    good_records = []
    mandatory_fields = ['label', 'maker', 'model', 'release']
    for i, r in enumerate(records):
      reject = 'Rejecting import of record %d: ' % i

      if self.missing_fields(mandatory_fields, r):
        f = reject + 'missing mandatory field.'
        self.logger.error(f)
        continue

      if r['label'] in k_map:
        f = (reject +
             'there is a pre-existing device with label %s. (in this batch).')
        self.logger.error(f % r['label'])
        continue
      elif r['label'] in self.preloaded_devices:
        f = reject + 'there is a pre-existing device with label %s.'
        self.logger.warn(f % r['label'])
        continue

      if r['barcode'] and r['barcode'] in self.known_barcodes:
        m = reject + 'there is a pre-existing object with barcode %s.'
        self.logger.warn(m % r['barcode'])
        continue

      if 'device_type' not in r:
        f = reject + 'missing device_type for record with label %s.'
        self.logger.error(f % r['label'])
        continue
      elif not issubclass(getattr(self.kb, r['device_type']), self.kb.Device):
        f = (reject +
             'device_type of device label %s is not a subclass of Device')
        self.logger.error(f % r['label'])
        continue
      elif r['device_type'] == 'GenotypingProgram':
        if not r.get('markers_set', None):
          f = (reject +
               'device label %s is a GenotypingProgram, missing markers_set')
          self.logger.error(f % r['label'])
          continue
        elif r['markers_set'] not in self.preloaded_markers_sets:
          f = (reject +
               'device label %s is a GenotypingProgram, unknown markers_set')
          self.logger.error(f % r['label'])
          continue

      k_map['label'] = r
      good_records.append(r)
    self.logger.info('done consistency checks')
    #--
    return good_records

  def process_chunk(self, chunk, otsv, study):
    devices = []
    for r in chunk:
      dklass = getattr(self.kb, r['device_type'])
      conf = {}
      for k in ['label', 'maker', 'model', 'release']:
        conf[k] = r[k]
      if r['location']:
        conf['physicalLocation'] = r['location']
      if r['barcode']:
        conf['barcode'] = r['barcode']
      if r['markers_set']:
        conf['snpMarkersSet'] = self.preloaded_markers_sets[r['markers_set']]
      devices.append(self.kb.factory.create(dklass, conf))
    self.kb.save_array(devices)
    #--
    for d in devices:
      otsv.writerow({'study' : study.label,
                     'label' : d.label,
                     'type'  : d.get_ome_table(),
                     'vid'   : d.id })

def canonize_records(args, records):
  fields = ['study', 'maker', 'model', 'release', 'device_type']
  for f in fields:
    if hasattr(args, f) and getattr(args,f) is not None:
      for r in records:
        r[f] = getattr(args, f)
  # specific fixes
  for k in ['location', 'barcode', 'markers_set']:
    for r in records:
      if not (k in r and (r[k] and r[k].upper() != 'NONE')):
        r[k] = None

help_doc = """
import new Device definitions into a virgil system.
"""

def make_parser_device(parser):
  parser.add_argument('--study', type=str,
                      help="""default conxtest study label.
                      It will over-ride the study column value""")
  parser.add_argument('--device-type', type=str,
                      choices=['Chip', 'Scanner',
                               'SoftwareProgram', 'GenotypingProgram'],
                      help="""default device type.  It will
                      over-ride the container_type column value, if any.
                      """)
  parser.add_argument('--maker', type=str,
                      help="""the device maker,
                      it will override the corresponding column""")
  parser.add_argument('--model', type=str,
                      help="""the device model,
                      it will override the corresponding column""")
  parser.add_argument('--release', type=str,
                      help="""the device release,
                      it will override the corresponding column""")


def import_device_implementation(logger, args):

  action_setup_conf = Recorder.find_action_setup_conf(args)

  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      keep_tokens=args.keep_tokens,
                      logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]

  canonize_records(args, records)

  o = csv.DictWriter(args.ofile,
                     fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t')
  o.writeheader()
  recorder.record(records, o)

  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('device', help_doc,
                            make_parser_device,
                            import_device_implementation))


