"""
Import Marker Alignments
========================

Will read in a tsv file with the following columns::

  marker_vid ref_genome chromosome pos      strand allele copies
  V0909090   hg18       10         82938938 True   A      1
  V0909091   hg18       1          82938999 True   A      2
  V0909092   hg18       1          82938938 True   B      2
  ...

FIXME discuss the fact that pos is with respect to 5' and thus, if the
marker had been alligned on the other strand, it is the responsability
of the aligner app to report the actual distance from 5', while, at
the same time, registering that the snp had actually been aligned on
the other strand.

Chromosome is an integer field with values in the range(1, 25) with 23
(X), 24 (Y) and 25(MT).

importer -i aligned_markers.tsv marker_alignment --ref-genome=hg18

"""

from bl.vl.kb import KBError
from core import Core, BadRecord
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

class Recorder(Core):
  """
  An utility class that handles the actual recording of marker definitions
  into VL.
  """
  def __init__(self, study_label,
               host=None, user=None, passwd=None,  keep_tokens=1,
               operator='Alfred E. Neumann'):
    """
    FIXME
    """
    self.logger = logger
    super(Recorder, self).__init__(host, user, passwd, study_label=study_label)
    #--
    #--
    device_label = ('importer.marker_definition.SNP-marker-definition-%s' %
                    (version))
    device = self.get_device(label=device_label,
                             maker='CRS4', model='importer', release='0.1')
    asetup = self.get_action_setup('importer.marker_alignment',
                                   {'study_label' : study_label,
                                    'operator' : operator})
    acat  = self.kb.ActionCategory.IMPORT
    self.action = self.kb.factory.create(self.kb.Action,
                                         {'setup' : asetup,
                                          'device' : device,
                                          'actionCategory' : acat,
                                          'operator' : operator,
                                          'context' : self.default_study,
                                          })
    #-- FIXME what happens if we do not have markers to save?
    self.action.save()

  def save_snp_marker_alignments(self, ref_genome, message, ifile):
    self.logger.info('start preloading known markers defs from kb')
    known_markers = self.kb.get_snp_marker_definitions()
    self.logger.info('done preloading known markers defs')
    self.logger.info('preloaded %d known markers defs' % len(known_markers))
    self.logger.info('will not check for rewritten alignment records')
    #--
    cnt = [0]
    known_markers_vid = set(known_markers['vid'])
    def ns(stream, cnt):
      for x in stream:
        if x['marker_vid'] not in known_markers_vid:
          msg = 'referencing marker with unkwnown vid %s' % x['marker_vid']
          self.logger.critical(msg)
          raise ValueError(msg)
        if ref_genome:
          x['ref_genome'] = ref_genome
        x['chromosome'] = int(x['chromosome'])
        x['pos'] = int(x['pos'])
        x['global_pos'] = 10**10 * x['chromosome'] + x['pos']
        x['strand'] = x['strand'].upper() in ['TRUE', '+']
        x['copies'] = int(x['copies'])
        cnt[0] += 1
        yield x
    tsv = csv.DictReader(ifile, delimiter='\t')
    #--
    pars = {'message' : message,
            'ref_genome' : ref_genome,
            'filename' : ifile.name}
    self.logger.info('start loading marker alignments from %s' % ifile.name)
    self.kb.add_snp_alignments(ns(tsv, cnt), op_vid=self.action.id)
    self.logger.info('done loading marker alignments. There were %s new alignments.'
                     % cnt[0])

#------------------------------------------------------------------------------

help_doc = """
import new markers alignments into VL.
"""

def make_parser_marker_alignment(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""context study label""")
  parser.add_argument('--ref-genome', type=str,
                      help="""reference genome used""")
  parser.add_argument('--message', type=str,
                      help="""optional informative message""",
                      default="")

def import_marker_alignment_implementation(args):
  if not (args.study):
    msg = 'missing context study label'
    logger.critical(msg)
    raise ValueError(msg)
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=1)
  recorder.save_snp_marker_alignments(args.ref_genome, args.message, args.ifile)

def do_register(registration_list):
  registration_list.append(('marker_alignment', help_doc,
                            make_parser_marker_alignment,
                            import_marker_alignment_implementation))


