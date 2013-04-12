# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Parse an Affymetrix SNP annotation file and extract info needed by the
marker set importer.
"""
import os, csv
from contextlib import nested

from bl.core.utils import NullLogger

from common import process_mask, MARKER_DEF_FIELDS


HELP_DOC = __doc__


class AffySNPReader(csv.DictReader):
  """
  Reads Affymetrix SNP annotation files.
  """
  def __init__(self, f):
    def comment_filter(f):
      for line in f:
        if not line.startswith("#"):
          yield line
    csv.DictReader.__init__(self, comment_filter(f))


def extract_data(fi, logger=None):
  logger = logger or NullLogger()
  bn = os.path.basename(fi.name)
  logger.info("processing %r" % bn)
  reader = AffySNPReader(fi)
  warn_count = 0
  for i, r in enumerate(reader):
    label = r['Probe Set ID']
    mask, allele_flip, error = process_mask(
      r['Flank'], r['Allele A'], r['Allele B']
      )
    if error:
      logger.warn("%s: %s" % (label, error))
      warn_count += 1
    yield label, mask, i, allele_flip
  logger.info("finished processing %s, %d warnings" % (bn, warn_count))


def write_output(stream, fo):
  writer = csv.writer(fo, delimiter="\t", lineterminator=os.linesep)
  writer.writerow(MARKER_DEF_FIELDS)
  for row in stream:
    writer.writerow(map(str, row))


def make_parser(parser):
  parser.add_argument('-i', '--input-file', metavar='FILE', required=True,
                      help='Affymetrix SNP annotation file')
  parser.add_argument('-o', '--output-file', metavar='FILE', required=True,
                      help='output file')


def main(logger, args):
  with nested(open(args.input_file), open(args.output_file, 'w')) as (f, outf):
    out_stream = extract_data(f, logger=logger)
    write_output(out_stream, outf)
  logger.info("all done")


def do_register(registration_list):
  registration_list.append(('convert_affy', HELP_DOC, make_parser, main))
