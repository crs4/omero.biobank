"""
Export Marker Definitions
==========================

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
    #--

  def dump_snp_marker_definitions(self, source, context, release, ofile):
    self.logger.info('start loading requested markers defs from kb')
    selector = ''
    if source:
      selector += "(source=='%s')&" % source
    if context:
      selector += "(context=='%s')&" % context
    if release:
      selector += "(release=='%s')&" % release
    selector = selector[:-1]
    markers = self.gkb.get_snp_marker_definitions(selector=selector)
    self.logger.info('done loading requested markers defs (%d)' %
                     len(markers))
    #--
    fieldnames = 'vid source context release label rs_label mask op_vid'.split()
    tsv = csv.DictWriter(ofile, fieldnames, delimiter='\t')
    tsv.writeheader()
    #--
    for x in markers:
      r = {}
      for k in fieldnames:
        r[k] = x[k]
      tsv.writerow(r)
#------------------------------------------------------------------------------

help_doc = """
export marker definitions as a tsv file.
"""

def make_parser_marker_definition(parser):
  parser.add_argument('--source', type=str,
                      help="""markers definition source""")
  parser.add_argument('--context', type=str,
                      help="""markers definition context""")
  parser.add_argument('--release', type=str,
                      help="""markers definition release""")

def export_marker_definition_implementation(args):
  if not (args.host and args.user and args.passwd):
    msg = 'missing command line options'
    logger.critical(msg)
    raise ValueError(msg)

  dumper = Dumper(host=args.host, user=args.user, passwd=args.passwd,
                  keep_tokens=args.keep_tokens)
  dumper.dump_snp_marker_definitions(args.source, args.context, args.release,
                                     args.ofile)

def do_register(registration_list):
  registration_list.append(('marker_definition', help_doc,
                            make_parser_marker_definition,
                            export_marker_definition_implementation))


