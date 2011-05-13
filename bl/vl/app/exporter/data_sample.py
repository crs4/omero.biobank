"""
Export Data Samples
===================

FIXME

"""

from bl.vl.sample.kb import KBError
from bl.vl.app.importer.core import Core, BadRecord
from version import version

import csv, json
import time, sys
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

class Dumper(Core):
  """
  An utility class that handles the actual dumping of marker definitions
  into VL.
  """
  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               operator='Alfred E. Neumann'):
    """
    FIXME
    """
    self.logger = logger
    super(Dumper, self).__init__(host, user, passwd)

  def dump(self, ofile):
    self.logger.info("dump: start preloading data samples")
    data_samples = self.skb.get_bio_samples(self.skb.DataSample)
    self.logger.info("dump: finished preloading data samples")
    for ds in data_samples:
      action = ds.action
      target = self.skb.SamplesContainerSlot(action.getTarget(), proxy=self.skb)
      print ds.name, ds.contrastQC, target.label, target.container.label


help_doc = """
FIXME
"""

def make_parser_data_sample(parser):
  pass


def export_data_sample_implementation(args):
  if not (args.host and args.user and args.passwd):
    msg = 'missing command line options'
    logger.critical(msg)
    raise ValueError(msg)

  dumper = Dumper(host=args.host, user=args.user, passwd=args.passwd,
                  keep_tokens=args.keep_tokens)
  dumper.dump(args.ofile)


def do_register(registration_list):
  registration_list.append(('data_sample', help_doc,
                            make_parser_data_sample,
                            export_data_sample_implementation))
