"""
Import Marker Definitions
==========================

FIXME: these docs are probably out of sync.

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

There are no collision controls.

It will output a a tsv file with the following columns::

   study    label     type    vid
   ASTUDY   SNP_A-xxx Marker  V000002222
   ...



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


class Recorder(Core):
  """
  An utility class that handles the actual recording of marker definitions
  into VL.
  """
  def __init__(self, study_label,
               host=None, user=None, passwd=None, keep_tokens=1,
               action_setup_conf=None,
               operator='Alfred E. Neumann', logger=None):
    """
    FIXME
    """
    super(Recorder, self).__init__(host, user, passwd, keep_tokens,
                                   study_label, logger=logger)
    self.action_setup_conf = action_setup_conf
    #--
    device_label = ('importer.marker_definition.SNP-marker-definition-%s' %
                    (version))
    device = self.get_device(label=device_label,
                             maker='CRS4', model='importer', release='0.1')
    asetup = self.get_action_setup(('importer.marker_definition-%f'
                                    % time.time()),
                                   json.dumps(self.action_setup_conf))
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



  def record(self, source, context, release, ifile, ofile):
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
        y = {'source': source, 'context' : context, 'release' : release,
             'label' : x['label'],
             'rs_label' : x['rs_label']}
        y['mask'] = convert_to_top(x['mask'])
        cnt[0] += 1
        yield y
    tsv = csv.DictReader(ifile, delimiter='\t')
    self.logger.info('start loading markers defs from %s' % ifile.name)
    vmap = self.kb.add_snp_marker_definitions(ns(tsv, cnt),
                                              op_vid=self.action.id)
    o = csv.DictWriter(ofile,
                       fieldnames=['study', 'label', 'type', 'vid'],
                       delimiter='\t')
    o.writeheader()
    for t in vmap:
      o.writerow({'study' : self.default_study.label,
                  'label' : t[0],
                  'type'  : 'Marker',
                  'vid'   : t[1]})
    self.logger.info('done loading markers defs there were %s new markers.' %
                     cnt[0])

#-----------------------------------------------------------------------------

help_doc = """
import new marker definitions into VL.
"""

def make_parser_marker_definition(parser):
  parser.add_argument('--study', type=str,
                      default='default_study',
                      help="""context study label""")

  parser.add_argument('--source', type=str,
                      help="""marker definition source""")
  parser.add_argument('--context', type=str,
                      help="""marker definition context""")
  parser.add_argument('--release', type=str,
                      help="""marker definition release""")

def import_marker_definition_implementation(logger, args):
  if not (args.study and args.source and args.context and args.release):
    msg = 'missing command line options'
    logger.critical(msg)
    sys.exit(1)

  action_setup_conf = Recorder.find_action_setup_conf(args)


  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      keep_tokens=args.keep_tokens, logger=logger)

  recorder.record(args.source, args.context, args.release,
                  args.ifile, args.ofile)

def do_register(registration_list):
  registration_list.append(('marker_definition', help_doc,
                            make_parser_marker_definition,
                            import_marker_definition_implementation))


