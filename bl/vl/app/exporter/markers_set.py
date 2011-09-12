"""
Export Markers Set Definitions
=============================

FIXME

"""

from bl.vl.kb import KBError
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

  def dump(self, list_markers, maker, model, release, ofile):
    if maker and model and release and list_markers:
      self.dump_markers_set(maker, model, release, ofile)
    else:
      mss = self.skb.get_bio_samples(self.skb.SNPMarkersSet)
      fieldnames = 'vid maker model release set_vid size'.split()
      tsv = csv.DictWriter(ofile, fieldnames, delimiter='\t')
      tsv.writeheader()
      for ms in mss:
        r = {'vid' : ms.id,
             'maker' : ms.maker, 'model' : ms.model, 'release' : ms.release,
             'set_vid' : ms.markersSetVID, 'size' : self.count_markers(ms)}
        tsv.writerow(r)

  def count_markers(self, ms):
    selector = "(vid=='%s')" % ms.markersSetVID
    return len(self.gkb.get_snp_markers_sets(selector=selector))

  def dump_markers_set(self, maker, model, release, ofile):
    selector = ("(maker=='%s')&(model=='%s')&(release=='%s')" %
                (maker, model, release))
    mrk_set = self.gkb.get_snp_markers_sets(selector=selector)
    selector = "(vid=='%s')" % mrk_set['vid'][0]
    mrks = self.gkb.get_snp_markers_set(selector=selector)
    #--
    self.logger.info('start preloading related markers')
    mrk_vids = mrks['marker_vid']
    selector = '|'.join(["(vid=='%s')" % k for k in mrk_vids])
    mrk_defs = self.gkb.get_snp_marker_definitions(selector=selector)
    assert len(mrk_defs) == len(mrk_vids)
    vid_to_rs = dict([ x for x in it.izip(mrk_defs['vid'],
                                          mrk_defs['rs_label'])])
    self.logger.info('done preloading related markers')
    #--
    fieldnames = 'rs_label marker_vid marker_indx allele_flip op_vid'.split()
    tsv = csv.DictWriter(ofile, fieldnames, delimiter='\t')
    tsv.writeheader()
    for m in mrks:
      r = {'rs_label' : vid_to_rs[m['marker_vid']],
           'marker_vid' : m['marker_vid'],
           'marker_indx' : m['marker_indx'],
           'allele_flip' : 'True' if m['allele_flip'] else 'False',
           'op_vid' : m['op_vid'],
           }
      tsv.writerow(r)

#-------------------------------------------------------------------------
help_doc = """
export markers set definition.  In default, it will only list known
SNPMarkersSet(s).  If maker, model and release and list-markers are
set, it will list all the markers of the set.
"""

def make_parser_markers_set(parser):
  parser.add_argument('--maker', type=str,
                      help="""markers_set maker""")
  parser.add_argument('--model', type=str,
                      help="""markers_set model""")
  parser.add_argument('--release', type=str,
                      help="""markers set release""")
  parser.add_argument('--list-markers', action='store_true',
                      default=False, help='List all markers in set.')

def import_markers_set_implementation(args):
  #--
  dumper = Dumper(host=args.host, user=args.user, passwd=args.passwd,
                keep_tokens=args.keep_tokens)
  dumper.dump(args.list_markers,
              args.maker, args.model, args.release, args.ofile)

def do_register(registration_list):
  registration_list.append(('markers_set', help_doc,
                            make_parser_markers_set,
                            import_markers_set_implementation))


