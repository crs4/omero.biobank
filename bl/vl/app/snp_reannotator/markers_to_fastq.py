"""
Convert VL marker definitions to fastq data.

A fastq record is generated for each allele where the sequence is
obtained by joining the flanks with that allele. The fastq id is built
by joining (with the '|' character) four fields: label, allele code (A
for the first one in the mask, B for the second, etc.), SNP offset
(i.e., length of the left flank) and original alleles (string).
"""
import os, argparse, csv
from contextlib import nested
from itertools import izip

from bl.core.utils import NullLogger
from bl.vl.utils.snp import split_mask
from common import SeqNameSerializer


HELP_DOC = __doc__
ALLELE_CODES = ('A', 'B', 'C', 'D')


def build_fastq_records(label, mask, name_serializer, logger=None):
  logger = logger or NullLogger()
  records = []
  try:
    lflank, alleles, rflank = split_mask(mask)
  except ValueError:
    logger.warn("%r: bad mask format, skipping" % (label,))
  else:
    snp_offset = len(lflank)
    for a, c in izip(alleles, ALLELE_CODES):
      seq = "%s%s%s" % (lflank, a, rflank)
      seq_id = name_serializer.serialize(label, c, snp_offset, alleles)
      r = ('@%s' % seq_id, seq, '+%s' % seq_id, '~'*len(seq))
      records.append(r)
  return records
    

def write_output(reader, outf, logger=None):
  logger = logger or NullLogger()
  seq_count = 0
  name_serializer = SeqNameSerializer()
  for r in reader:
    fastq_records = build_fastq_records(
      r['label'], r['mask'], name_serializer, logger=logger
      )
    seq_count += len(fastq_records)
    for r in fastq_records:
      outf.write("%s\n" % "\n".join(r))
  return seq_count


def make_parser(parser):
  parser.add_argument('-i', '--input-file', metavar='FILE', required=True,
                      help='VL marker definitions file')
  parser.add_argument('-o', '--output-file', metavar='FILE', required=True,
                      help='output file')


def main(logger, args):  
  with nested(open(args.input_file), open(args.output_file, 'w')) as (f, outf):
    bn = os.path.basename(args.input_file)
    logger.info("processing %r" % bn)
    reader = csv.DictReader(f, delimiter="\t")
    seq_count = write_output(reader, outf, logger=logger)
  logger.info("fastq records generated from %r: %d" % (bn, seq_count))


def do_register(registration_list):
  registration_list.append(('markers_to_fastq', HELP_DOC, make_parser, main))
