# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Parse an Illumina SNP annotation file and extract info needed by the
marker set importer.
"""
import os
from contextlib import nested

from bl.core.utils import NullLogger
from bl.core.seq.utils.baseops import COMPLEMENT
from bl.core.io.illumina import IllSNPReader

from common import process_mask, write_mdef


HELP_DOC = __doc__


def extract_data(fi, logger=None):
  logger = logger or NullLogger()
  bn = os.path.basename(fi.name)
  logger.info("processing %r" % bn)
  reader = IllSNPReader(fi)
  warn_count = 0
  for i, r in enumerate(reader):
    label = r['IlmnID']
    # alleles are the same as those extracted from the mask if strand
    # is TOP; if strand is BOT they are their complement (NOT reversed)
    allele_a, allele_b = r['SNP'].strip("[]").split("/")
    if r['IlmnStrand'] == 'BOT':
      allele_a, allele_b = COMPLEMENT[allele_a], COMPLEMENT[allele_b]
    mask, allele_flip, error = process_mask(
      r['TopGenomicSeq'], allele_a, allele_b
      )
    if error:
      logger.warn("%s: %s" % (label, error))
      warn_count += 1
    yield label, mask, i, allele_flip
  logger.info("finished processing %s, %d warnings" % (bn, warn_count))


def make_parser(parser):
  parser.add_argument('-i', '--input-file', metavar='FILE', required=True,
                      help='Illumina SNP annotation file')
  parser.add_argument('-o', '--output-file', metavar='FILE', required=True,
                      help='output file')


def main(logger, args):  
  with nested(open(args.input_file), open(args.output_file, 'w')) as (f, outf):
    out_stream = extract_data(f, logger=logger)
    write_mdef(out_stream, outf)
  logger.info("all done")


def do_register(registration_list):
  registration_list.append(('convert_ill', HELP_DOC, make_parser, main))
