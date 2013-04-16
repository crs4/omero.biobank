# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Parse dbSNP files and extract info needed by the marker set importer.

dbSNP data is read from fasta dumps downloaded from:
ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606/rs_fasta

NOTE: this tool does not deal with the trailing 'comment':

# ================
# File created at:
# `date`
# ================

found in original files downloaded from NCBI. Such 'comments' are not
legal in FASTA files. This means that, with no pre-processing, those
lines are included in the last sequence (however, they might not end
up in the index because of flank truncation).
"""
import os

from bl.core.seq.io import DbSnpReader
from bl.core.utils import NullLogger
from common import process_mask, write_mdef


HELP_DOC = __doc__


def build_mask(lflank, alleles, rflank, mask_size):
  alleles = "/".join(alleles)
  if mask_size >= 2:
    L, R = len(lflank), len(rflank)
    N = mask_size / 2
    if L < N:
      R = 2*N - L
    elif R < N:
      L = 2*N - R
    else:
      L = R = N
    lflank, rflank = lflank[-L:], rflank[:R]
  return "%s[%s]%s" % (lflank, alleles, rflank)


def extract_data(fi, mask_size, logger=None):
  logger = logger or NullLogger()
  bn = os.path.basename(fi.name)
  logger.info("processing %r" % bn)
  reader = DbSnpReader(fi, logger=logger)
  warn_count = 0
  for i, (label, lflank, alleles, rflank) in enumerate(reader):
    alleles = alleles.split("/")
    mask = build_mask(lflank, alleles, rflank, mask_size)
    mask, allele_flip, error = process_mask(mask, alleles[0], alleles[1])
    if error:
      logger.warn("%s: %s" % (label, error))
      warn_count += 1
    yield label, mask, i, allele_flip
  logger.info("finished processing %s, %d warnings" % (bn, warn_count))


def make_parser(parser):
  parser.add_argument('-d', '--dbsnp-dir', metavar='DIR', required=True,
                      help='a directory containing dbSNP (.fas) files')
  parser.add_argument('-o', '--output-file', metavar='FILE', required=True,
                      help='output file')
  parser.add_argument('--mask-size', type=int, metavar='INT', default=200,
                      help='maximum output mask size (<=1: do not trim)')


def main(logger, args):
  db_filenames = [os.path.join(args.dbsnp_dir, fn)
                  for fn in os.listdir(args.dbsnp_dir)
                  if fn.endswith('fas')]
  logger.info("found %d dbSNP files" % len(db_filenames))
  with open(args.output_file, 'w') as outf:
    for i, fn in enumerate(db_filenames):
      header = i == 0
      with open(fn) as f:
        out_stream = extract_data(f, args.mask_size, logger=logger)
        write_mdef(out_stream, outf, header=header)
    logger.info("all done")


def do_register(registration_list):
  registration_list.append(('convert_dbsnp', HELP_DOC, make_parser, main))
