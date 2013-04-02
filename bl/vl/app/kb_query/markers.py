# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Extract marker data from KB
===========================
"""

import os, csv

from bl.vl.app.importer.core import Core


CSV_OPTS = {
  "delimiter": '\t',
  "lineterminator": os.linesep,
  }


class Markers(Core):

  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
               mset_label=None, operator='Alfred E. Neumann', logger=None):
    self.logger = logger
    super(Markers, self).__init__(host, user, passwd, keep_tokens=keep_tokens)
    self.mset = self.kb.get_snp_markers_set(mset_label)
    if not self.mset:
      raise ValueError('unknown marker set %s' % mset_label)

  def dump_definitions(self, ofile):
    self.logger.info('dumping marker definitions for %s' % self.mset.label)
    self.mset.load_markers()
    fieldnames = ['label', 'mask', 'index', 'allele_flip']
    writer = csv.writer(ofile, **CSV_OPTS)
    writer.writerow(fieldnames)
    for row in self.mset.markers:
      writer.writerow([str(row[n]) for n in fieldnames])
    self.logger.info('marker definitions dumped to %s' % ofile.name)

  def dump_alignments(self, ofile, ref_genome):
    self.logger.info('dumping marker alignments for %s' % self.mset.label)
    self.mset.load_alignments(ref_genome)
    fieldnames = [
      'marker_vid', 'chromosome', 'pos', 'allele', 'strand', 'copies'
      ]
    writer = csv.writer(ofile, **CSV_OPTS)
    writer.writerow(fieldnames)
    for row in self.mset.aligns:
      writer.writerow([str(row[n]) for n in fieldnames])
    self.logger.info('marker alignments dumped to %s' % ofile.name)


help_doc = """
Extract marker-related info from the KB
"""


def make_parser(parser):
  # FIXME: allow ms selection by (maker, model, release)
  parser.add_argument('--marker-set', metavar="STRING",
                      help="marker set label", required=True)
  parser.add_argument('--alignments', metavar="REF_GENOME",
                      help="also dump alignment info wrt REF_GENOME")
  parser.add_argument('--alignments-file', metavar="FILE",
                      help="dump alignment info to this file")


def implementation(logger, host, user, passwd, args):
  markers = Markers(host=host, user=user, passwd=passwd, logger=logger,
                    keep_tokens=args.keep_tokens, mset_label=args.marker_set)
  markers.dump_definitions(args.ofile)
  if args.alignments:
    if not args.alignments_file:
      alignments_fn = "%s_al%s" % os.path.splitext(args.ofile.name)
      alignments_file = open(alignments_fn, "w")
    else:
      alignments_file = open(args.alignments_file)
    markers.dump_alignments(alignments_file, args.alignments)
  logger.info("all done")


def do_register(registration_list):
  registration_list.append(('markers', help_doc, make_parser, implementation))
