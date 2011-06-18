"""
Import of Device(s)
===================

Will read in a tsv file with the following columns::

  label barcode maker model release location
  pula01  8989898 Affymetrix  GeneChip Scanner 3000 7G  Pula bld. 5
  chip001 8329482 Affymetrix  Genome-Wide Human SNP Array 6.0  None

All devices have a label, an optional barcode, a maker, a model and a
release, and, possibly a physical location. In the example above, in
the first line we have defined a scanner, which is physically in the
lab in Pula, building 5.  The following line defines a chip.

FIXME this starts to become somewhat baroque. How does one write in an
action that it has used chip xxx on scanner yyy?
"""

from bl.vl.sample.kb import KBError
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
  An utility class that handles the actual recording of Device(s)
  into VL, including Device(s) generation as needed.
  """
  def __init__(self,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann'):
    """
    FIXME
    """
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   None)
    self.batch_size = batch_size
    self.operator = operator

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
    self.preload_devices()
    #--
    records = self.do_consistency_checks(records)
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(c)
      self.logger.info('done processing chunk %d' % i)


  def preload_devices(self):
    self.logger.info('start prefetching devices')
    self.known_devices = {}
    self.known_barcodes = []
    devices = self.kb.get_objects(self.kb.Device)
    for d in devices:
      self.known_devices[d.label] = d
      if hasattr(d, 'barcode') and d.barcode is not None:
        self.known_barcodes.append(d.barcode)
    self.logger.info('there are %d Device(s) in the kb'
                     % (len(self.known_devices)))

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    #--
    good_records = []
    for i, r in enumerate(records):
      reject = ' Rejecting import of record %d.' % i
      if r['barcode'] in self.known_barcodes:
        m = 'there is a pre-existing object with barcode %s.' + reject
        self.logger.warn(m % r['barcode'])
        continue
      if self.known_devices.has_key(r['label']):
        f = 'there is a pre-existing device with label %s.' + reject
        self.logger.warn(f % r['label'])
        continue
      k = 'maker'
      if not k in r:
        f = 'missing %s.' + reject
        self.logger.error(f % k)
        continue
      k = 'model'
      if not k in r:
        f = 'missing %s.' + reject
        self.logger.error(f % k)
        continue
      k = 'release'
      if not k in r:
        f = 'missing %s.' + reject
        self.logger.error(f % k)
        continue
      good_records.append(r)
    self.logger.info('done consistency checks')
    #--
    return good_records

  def process_chunk(self, chunk):
    devices = []
    for r in chunk:
      conf = {}
      for k in ['label', 'maker', 'model', 'release']:
        conf[k] = r[k]
      if 'location'  in r and r['location'] is not 'None':
        conf['physicalLocation'] = r['location']
      if 'barcode' in r and r['barcode'] is not 'None':
        conf['barcode'] = r['barcode']
      devices.append(self.kb.factory.create(self.kb.Device, conf))
    self.kb.save_array(devices)
    #--
    for d in devices:
      self.logger.info('saved %s[%s,%s,%s] as %s.'
                       % (d.label, d.maker, d.model, d.release, d.id))

help_doc = """
import new Device definitions into a virgil system.
"""

def make_parser_device(parser):
  pass

def import_device_implementation(args):
  # FIXME it is very likely that the following can be directly
  # implemented as a validation function in the parser definition above.
  recorder = Recorder(host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  recorder.record(records)
  logger.info('done processing file %s' % args.ifile.name)


def do_register(registration_list):
  registration_list.append(('device', help_doc,
                            make_parser_device,
                            import_device_implementation))


