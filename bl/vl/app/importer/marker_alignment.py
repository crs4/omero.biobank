"""
Import Marker Alignments
========================

Will read in a tsv file with the following columns::

  label ref_genome chromosome pos      strand allele copies
  rs6576   hg18       10         82938938 True   A      1
  rs6576   hg18       1          82938999 True   A      2
  rs6576   hg18       1          82938938 True   B      2
  ...

importer -i aligned_markers.tsv marker_alignment --ref-genome=hg18

"""

from bl.vl.sample.kb import KBError
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
    super(Recorder, self).__init__(host, user, passwd)
    #--
    s = self.skb.get_study_by_label(study_label)
    if not s:
      self.logger.critical('No known study with label %s' % study_label)
      sys.exit(1)
    self.study = s
    #-------------------------
    self.device = self.get_device('importer-0.0', 'CRS4', 'IMPORT', '0.0')
    self.asetup = self.get_action_setup('importer-version-%s-%s-%f' %
                                        (version, "SNPMarkersSet", time.time()),
                                        # FIXME the json below should
                                        # record the app version, and the
                                        # parameters used.  unclear if we
                                        # need to register the file we load
                                        # data from, since it is, most
                                        # likely, a transient object.
                                        json.dumps({'operator' : operator,
                                                    'host' : host,
                                                    'user' : user}))
    self.acat  = self.acat_map['IMPORT']
    self.operator = operator

  def create_action(self, description):
    return self.create_action_helper(self.skb.Action, description,
                                     self.study, self.device, self.asetup,
                                     self.acat, self.operator, None)

  def save_snp_marker_alignments(self, ref_genome, message, ifile):
    self.logger.info('start preloading known markers defs from kb')
    known_markers = self.gkb.get_snp_marker_definitions()
    if len(known_markers) > 0:
      known_markers = dict(it.izip(known_markers['label'],
                                   known_markers['vid']))
    self.logger.info('done preloading known markers defs')
    self.logger.info('preloaded %d known markers defs' % len(known_markers))
    self.logger.info('will not check for rewritten alignment records')
    #--
    cnt = [0]
    def ns(stream, cnt):
      for x in stream:
        if not known_markers.has_key(x['label']):
          msg = 'referencing marker with unkwnon label %s' % x['label']
          self.logger.critical(msg)
          raise ValueError(msg)
        if ref_genome:
          x['ref_genome'] = ref_genome
        x['marker_vid'] = known_markers[x['label']]
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
    action = self.create_action(description=json.dumps(pars))
    self.logger.info('start loading marker alignments from %s' % ifile.name)
    self.gkb.add_snp_alignments(ns(tsv, cnt), op_vid=action.id)
    self.logger.info('done loading marker alignments. There were %s new alignments.'
                     % cnt[0])

#------------------------------------------------------------------------------------

help_doc = """
import new markers alignments into VL.
"""

def make_parser_marker_alignment(parser):
  parser.add_argument('-S', '--study', type=str,
                      help="""context study label""")
  parser.add_argument('--ref-genome', type=str,
                      help="""reference genome used""")
  parser.add_argument('--message', type=str,
                      help="""reference genome used""",
                      default="")

def import_marker_alignment_implementation(args):
  if not (args.study):
    msg = 'missing context study label'
    logger.critical(msg)
    raise ValueError(msg)
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  recorder.save_snp_marker_alignments(args.ref_genome, args.message, args.ifile)

def do_register(registration_list):
  registration_list.append(('marker_alignment', help_doc,
                            make_parser_marker_alignment,
                            import_marker_alignment_implementation))


