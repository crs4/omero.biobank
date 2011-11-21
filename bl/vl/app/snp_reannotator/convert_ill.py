"""
Convert Illumina SNP annotation files to the VL marker definition format.
"""
import os, csv
from contextlib import nested

from bl.core.utils import NullLogger
from bl.core.io.illumina import IllSNPReader
from common import check_mask, MARKER_DEF_FIELDS


HELP_DOC = __doc__


def write_output(reader, outf, logger=None):
  logger = logger or NullLogger()
  bad_count = 0
  for r in reader:
    label = r['IlmnID']
    if r['Name'].startswith('rs'):
      rs_label = r['Name']
    else:
      rs_label = 'None'
    mask = r['TopGenomicSeq']
    # alleles are the same as those extracted from the mask if strand
    # is TOP; if strand is BOT they are their complement (NOT reversed)
    allele_a, allele_b = r['SNP'].strip("[]").split("/")
    problem = check_mask(mask)
    if problem:
      mask = 'None'
      logger.warn("%r: %s, setting mask to 'None'" % (label, problem))
      bad_count += 1
    outf.write("%s\t%s\t%s\t%s\t%s\n" %
               (label, rs_label, mask, allele_a, allele_b))
  return bad_count


def make_parser(parser):
  parser.add_argument('-i', '--input-file', metavar='FILE', required=True,
                      help='Illumina SNP annotation file')
  parser.add_argument('-o', '--output-file', metavar='FILE', required=True,
                      help='output file')


def main(logger, args):  
  with nested(open(args.input_file), open(args.output_file, 'w')) as (f, outf):
    bn = os.path.basename(args.input_file)
    logger.info("processing %r" % bn)
    outf.write("\t".join(MARKER_DEF_FIELDS)+"\n")
    reader = IllSNPReader(f)
    bad_count = write_output(reader, outf, logger=logger)
  logger.info("bad masks for %r: %d" % (bn, bad_count))


def do_register(registration_list):
  registration_list.append(('convert_ill', HELP_DOC, make_parser, main))
