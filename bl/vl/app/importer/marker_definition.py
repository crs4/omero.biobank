"""
Import Marker Definitions
=========================

Will read in a tsv file with the following columns::

  label         rs_label   mask
  SNP_A-1780419 rs6576700  <lflank>[A/B]<rflank>
  ...

where label is the unique label for this marker in the (source,
context, release) context, rs_label is its dbSNP label ('None' if not
defined or not known). The mask column contains the SNP definition.

It will, for each row, convert mask to the TOP strand following
Illumina conventions and then save a record for it in the KB.

.. todo::

   add a reference to the data structure documentation.

It will output a a tsv file with the following columns::

   study    label     type    vid
   ASTUDY   SNP_A-xxx Marker  V000002222
   ...
"""

import csv, json, time, os

from bl.vl.utils.snp import convert_to_top

import core
from version import version


class Recorder(core.Core):

  def __init__(self, study_label,
               host=None, user=None, passwd=None, keep_tokens=1,
               action_setup_conf=None,
               operator='Alfred E. Neumann', logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.action_setup_conf = action_setup_conf
    device_label = ('importer.marker_definition.SNP-marker-definition-%s' %
                    version)
    device = self.get_device(label=device_label,
                             maker='CRS4', model='importer', release='0.1')
    asetup = self.get_action_setup(
      'importer.marker_definition-%f' % time.time(),
      json.dumps(self.action_setup_conf)
      )
    acat = self.kb.ActionCategory.IMPORT
    conf = {
      'setup': asetup,
      'device': device,
      'actionCategory': acat,
      'operator': operator,
      'context': self.default_study,
      }
    self.action = self.kb.factory.create(self.kb.Action, conf)
    #-- FIXME what happens if we do not have markers to save?
    self.action.save()

  def record(self, source, context, release, ifile, ofile,
             ref_genome, dbsnp_build):
    self.logger.info('start preloading known marker defs from kb')
    selector = '(source=="%s")' % source
    if context:
      selector += '&(context=="%s")' % context
    if release:
      selector += '&(release=="%s")' % release
    known_markers = self.kb.get_snp_marker_definitions(selector=selector,
                                                       col_names=['label'])
    if len(known_markers) > 0:
      known_markers = known_markers['label']
    known_markers = set(known_markers)
    self.logger.info('preloaded %d known markers defs' % len(known_markers))
    cnt = {"good": 0, "bad": 0}
    def ns(stream, cnt):
      for x in stream:
        k = x['label']
        if k in known_markers:
          self.logger.warn('%s: already loaded' % x['label'])
          continue
        y = {
          'source': source,
          'context': context,
          'release': release,
          'label': x['label'],
          'rs_label': x['rs_label'],
          'ref_rs_genome': ref_genome,
          'dbSNP_build': dbsnp_build,
          }
        try:
          y['mask'] = convert_to_top(x['mask'])
        except ValueError:
          y['mask'] = x['mask']
          self.logger.warn('%s: could not convert mask to top' % x['label'])
          cnt["bad"] += 1
        else:
          cnt["good"] += 1
        yield y
        known_markers.add(k)
    tsv = csv.DictReader(ifile, delimiter='\t')
    self.logger.info('start loading markers defs from %s' % ifile.name)
    vmap = self.kb.add_snp_marker_definitions(ns(tsv, cnt), action=self.action)
    o = csv.DictWriter(ofile, delimiter='\t', lineterminator=os.linesep,
                       fieldnames=['study', 'label', 'type', 'vid'])
    o.writeheader()
    for t in vmap:
      o.writerow({
        'study': self.default_study.label,
        'label': t[0],
        'type': 'Marker',
        'vid': t[1],
        })
    self.logger.info('done loading markers defs')
    self.logger.info('good masks: %d' % cnt["good"])
    self.logger.info('bad masks: %d' % cnt["bad"])


help_doc = """
import new marker definitions into the KB.
"""


def make_parser(parser):
  parser.add_argument('--study', metavar="STRING", required=True,
                      help="study label")
  parser.add_argument('--ref-genome', metavar="STRING", required=True,
                      help="reference genome, e.g., hg19")
  parser.add_argument('--dbsnp-build', type=int, required=True,
                      help="dbSNP build, e.g., 134", metavar="INT")
  parser.add_argument('--source', metavar="STRING", required=True,
                      help="marker definition source")
  parser.add_argument('--context', metavar="STRING", required=True,
                      help="marker definition context")
  parser.add_argument('--release', metavar="STRING", required=True,
                      help="marker definition release")


def implementation(logger, args):
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      keep_tokens=args.keep_tokens, logger=logger)
  recorder.record(args.source, args.context, args.release,
                  args.ifile, args.ofile,
                  args.ref_genome, args.dbsnp_build)


def do_register(registration_list):
  registration_list.append(('marker_definition', help_doc, make_parser,
                            implementation))
