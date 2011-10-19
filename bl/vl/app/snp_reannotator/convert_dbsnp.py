"""
Convert dbSNP files to the VL marker definition format.
"""
import os

from bl.core.seq.io import DbSnpReader
from bl.core.utils import NullLogger
from common import POSSIBLE_ALLELES, MARKER_DEF_FIELDS


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


def write_output(db_snp_reader, outf, mask_size, logger=None):
  logger = logger or NullLogger()
  bad_count = 0
  for rs_label, lflank, alleles, rflank in db_snp_reader:
    alleles = alleles.split("/")
    if 2 <= len(alleles) <= 4 and set(alleles) <= POSSIBLE_ALLELES:
      mask = build_mask(lflank, alleles, rflank, mask_size)
      outf.write("%s\t%s\t%s\n" % (rs_label, rs_label, mask))
    else:
      logger.warn("%r: bad alleles %r, skipping" % (rs_label, alleles))
      bad_count += 1
  return bad_count


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
    outf.write("\t".join(MARKER_DEF_FIELDS)+"\n")
    for fn in db_filenames:
      bn = os.path.basename(fn)
      logger.info("processing %r" % bn)
      with open(fn) as f:
        db_snp_reader = DbSnpReader(f, logger=logger)
        bad_count = write_output(db_snp_reader, outf, args.mask_size,
                                 logger=logger)
      logger.info("bad masks for %r: %d" % (bn, bad_count))


def do_register(registration_list):
  registration_list.append(('convert_dbsnp', HELP_DOC, make_parser, main))
