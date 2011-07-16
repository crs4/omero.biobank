"""
Import Marker Definitions
==========================

Will read in a tsv file with the following columns::

  label         rs_label   mask                   strand allele_a allele_b
  SNP_A-1780419 rs6576700  <lflank>[A/B]<rflank>  TOP    A        B
  ...

Where label is supposed to be the unique label for this marker in the
(source, context, release) context, rs_label is the dbSNP db label for
this snp (it could be the string ``None`` if it not defined/not
known). The column mask contains the SNP definition. The strand column
could either be the actual 'illumina style' strand used to define the
alleles in the alleles columns, or the string 'None' which means that
the alleles in the allele column are defined against the mask in the
mask column.

It will, for each row, convert mask to the TOP strand following
Illumina conventions and then save a record for it in VL.

The saved tuple is (source, context, release, label, rs_label,
TOP_mask)

The only available collision control is on the rs_label.

Example usage::

  bash$ importer -i markers.tsv marker_definition \
                 --source='Affymetrix' --context='GenomeWide-6.0' \
                 --release='na28'

"""

from bl.vl.kb import KBError
from bl.vl.utils.snp import convert_to_top
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
  An utility class that handles the actual recording of marker definitions
  into VL.
  """
  def __init__(self, study_label,
               host=None, user=None, passwd=None, keep_tokens=1,
               operator='Alfred E. Neumann'):
    """
    FIXME
    """
    self.logger = logger
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   study_label)
    #--
    device_label = ('importer.marker_definition.SNP-marker-definition-%s' %
                    (version))
    device = self.get_device(label=device_label,
                             maker='CRS4', model='importer', release='0.1')
    asetup = self.get_action_setup('importer.marker_definition',
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


  def save_snp_marker_definitions(self, source, context, release, ifile):
    self.logger.info('start preloading known markers defs from kb')
    known_markers = self.kb.get_snp_marker_definitions()
    if len(known_markers) > 0:
      known_markers = known_markers['rs_label']

    self.logger.info('done preloading known markers defs')
    self.logger.info('preloaded %d known markers defs' % len(known_markers))
    #--
    cnt = [0]
    def ns(stream, cnt):
      for x in stream:
        if (len(known_markers) > 0
            and not (x['rs_label'] != known_markers).all()):
          self.logger.warn('marker with rs_label %s is in kb, skipping it.' %
                           x['rs_label'])
          continue
        #--
        y = {'source': source, 'context' : context, 'release' : release,
             'label' : x['label'], 'rs_label' : x['rs_label']}
        y['mask'] = convert_to_top(x['mask'])
        cnt[0] += 1
        yield y
    tsv = csv.DictReader(ifile, delimiter='\t')
    self.logger.info('start loading markers defs from %s' % ifile.name)
    self.kb.add_snp_marker_definitions(ns(tsv, cnt), op_vid=self.action.id)
    self.logger.info('done loading markers defs there were %s new markers.' % cnt[0])

#-----------------------------------------------------------------------------

help_doc = """
import new marker definitions into VL.
"""

def make_parser_marker_definition(parser):
  parser.add_argument('-S', '--study', type=str,
                      default='default_study',
                      help="""context study label""")

  parser.add_argument('--source', type=str,
                      help="""marker definition source""")
  parser.add_argument('--context', type=str,
                      help="""marker definition context""")
  parser.add_argument('--release', type=str,
                      help="""marker definition release""")

def import_marker_definition_implementation(args):
  if not (args.study and args.source and args.context and args.release):
    msg = 'missing command line options'
    logger.critical(msg)
    raise ValueError(msg)
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      keep_tokens=args.keep_tokens)
  recorder.save_snp_marker_definitions(args.source, args.context, args.release,
                                       args.ifile)

def do_register(registration_list):
  registration_list.append(('marker_definition', help_doc,
                            make_parser_marker_definition,
                            import_marker_definition_implementation))


