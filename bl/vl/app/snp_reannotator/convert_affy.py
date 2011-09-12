"""
Convert Affymetrix SNP annotation files to the VL marker definition format.
"""
import os, argparse, csv
from contextlib import nested

from bl.core.utils import NullLogger
from bl.vl.utils.snp import split_mask
from common import POSSIBLE_ALLELES, MARKER_DEF_FIELDS


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
    rs_label = r['dbSNP RS ID']
    mask = r['Flank']
    try:
      lflank, alleles, rflank = split_mask(mask)
    except ValueError:
      problem = "bad mask format"
    else:
      if not(2 <= len(alleles) <= 4 and set(alleles) <= POSSIBLE_ALLELES):
        problem = "bad alleles: %r" % list(alleles)
      else:
        problem = None
    if not problem:
      outf.write("%s\t%s\t%s\n" % (label, rs_label, mask))
    else:
      logger.warn("%r: %s, skipping" % (label, problem))
      bad_count += 1
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
