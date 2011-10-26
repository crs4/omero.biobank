"""
Convert Affymetrix SNP annotation files to the VL marker definition format.
"""
import os, csv
from contextlib import nested

from bl.core.utils import NullLogger
from common import check_mask, MARKER_DEF_FIELDS


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


def write_output(reader, outf, logger=None):
  logger = logger or NullLogger()
  bad_count = 0
  for r in reader:
    label = r['Probe Set ID']
    if r['dbSNP RS ID'].startswith('rs'):
      rs_label = r['dbSNP RS ID']
    else:
      rs_label = 'None'
    mask = r['Flank']
    problem = check_mask(mask)
    if problem:
      mask = 'None'
      logger.warn("%r: %s, setting mask to 'None'" % (label, problem))
      bad_count += 1
    outf.write("%s\t%s\t%s\t%s\t%s\n" %
               (label, rs_label, mask, r['Allele A'], r['Allele B']))
  return bad_count


def make_parser(parser):
  parser.add_argument('-i', '--input-file', metavar='FILE', required=True,
                      help='Affymetrix SNP annotation file')
  parser.add_argument('-o', '--output-file', metavar='FILE', required=True,
                      help='output file')


def main(logger, args):  
  with nested(open(args.input_file), open(args.output_file, 'w')) as (f, outf):
    bn = os.path.basename(args.input_file)
    logger.info("processing %r" % bn)
    outf.write("\t".join(MARKER_DEF_FIELDS)+"\n")
    reader = AffySNPReader(f)
    bad_count = write_output(reader, outf, logger=logger)
  logger.info("bad masks for %r: %d" % (bn, bad_count))


def do_register(registration_list):
  registration_list.append(('convert_affy', HELP_DOC, make_parser, main))
