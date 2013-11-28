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
    self.mset = self.kb.genomics.get_markers_array(mset_label)
    if not self.mset:
      raise ValueError('unknown marker set %s' % mset_label)

  def dump_definitions(self, ofile):
    self.logger.info('dumping marker definitions for %s' % self.mset.label)
    n_rows = self.kb.genomics.get_number_of_markers(self.mset)
    self.logger.info('there are %s markers definitions' % n_rows)
    rows = self.kb.genomics.get_markers_array_rows(self.mset)
    fieldnames = ['label', 'mask', 'index', 'permutation']
    writer = csv.writer(ofile, **CSV_OPTS)
    writer.writerow(fieldnames)
    for row in rows:
      writer.writerow(map(str, row))
    self.logger.info('marker definitions dumped to %s' % ofile.name)


help_doc = """
Extract marker-related info from the KB
"""


def make_parser(parser):
  # FIXME: allow ms selection by (maker, model, release)
  parser.add_argument('--marker-set', metavar="STRING",
                      help="marker set label", required=True)

def implementation(logger, host, user, passwd, args):
  markers = Markers(host=host, user=user, passwd=passwd, logger=logger,
                    keep_tokens=args.keep_tokens, mset_label=args.marker_set)
  markers.dump_definitions(args.ofile)
  logger.info("all done")


def do_register(registration_list):
  registration_list.append(('markers', help_doc, make_parser, implementation))
