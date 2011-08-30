"""
Convert dbSNP files to the format expected by SEAL's realign_snp tool.
"""
import os, argparse
from bl.core.seq.io import DbSnpReader
from bl.core.utils import NullLogger


HELP_DOC = __doc__


POSSIBLE_ALLELES = frozenset(['A', 'C', 'G', 'T'])


def write_output(db_snp_reader, outf, logger=None):
  logger = logger or NullLogger()
  bad_count = 0
  for rs_label, lflank, alleles, rflank in db_snp_reader:
    alleles = alleles.split("/")
    if 2 <= len(alleles) <= 4 and set(alleles) <= POSSIBLE_ALLELES:
      alleles = "/".join(alleles)
      outf.write("%s\t%s[%s]%s\n" % (rs_label, lflank, alleles, rflank))
    else:
      logger.warn("%r: bad alleles %r, skipping" % (rs_label, alleles))
      bad_count += 1
  return bad_count


def make_parser(parser):
  parser.add_argument('-d', '--dbsnp-dir', metavar='DIR', required=True,
                      help='a directory containing dbSNP (.fas) files')
  parser.add_argument('-o', '--output-file', metavar='FILE', required=True,
                      help='output file')


def main(logger, args):
  db_filenames = [os.path.join(args.dbsnp_dir, fn)
                  for fn in os.listdir(args.dbsnp_dir)
                  if fn.endswith('fas')]
  logger.info("found %d dbSNP files" % len(db_filenames))
  with open(args.output_file, 'w') as outf:
    outf.write("vid\tmask\n")
    for fn in db_filenames:
      bn = os.path.basename(fn)
      logger.info("processing %r" % bn)
      with open(fn) as f:
        db_snp_reader = DbSnpReader(f, logger=logger)
        bad_count = write_output(db_snp_reader, outf, logger=logger)
      logger.info("bad alleles for %r: %d" % (bn, bad_count))


def do_register(registration_list):
  registration_list.append(('convert_dbsnp', HELP_DOC, make_parser, main))
