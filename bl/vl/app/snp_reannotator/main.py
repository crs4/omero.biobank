import sys, argparse

"""
Check chip manufacturer's marker annotations against NCBI dbSNP.

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

Given a SNP defined as lflank[alleles]rflank, a reference genome
DB and a requested flanking size L::

  1. align [lflankArflank for A in alleles] to DB, with multiple
     alignments flagged as suspicious and kept for further processing;

  2. extract a region with L length symmetric flanks around the SNP
     alignment position: this will be a key into an on-file dictionary
     where values will be tuples of sequences that correspond to that
     key.

Given the above mask canonization strategy::

  1. build a canonized dbSNP wrt DB and use the resulting key to
     populate the index

  2. for each genotyping platform, canonize masks as described above
     and use the canonized masks to perform a lookup into the index.

The alignment step is external.
"""

import logging
import convert_dbsnp, convert_affy, convert_ill, markers_to_fastq, \
       convert_sam, build_index, lookup_index


LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']


class App(object):
  
  def __init__(self):
    self.supported_submodules = []
    convert_dbsnp.do_register(self.supported_submodules)
    convert_affy.do_register(self.supported_submodules)
    convert_ill.do_register(self.supported_submodules)
    markers_to_fastq.do_register(self.supported_submodules)
    convert_sam.do_register(self.supported_submodules)
    build_index.do_register(self.supported_submodules)
    lookup_index.do_register(self.supported_submodules)

  def make_parser(self):
    parser = argparse.ArgumentParser(description="A SNP reannotator app")
    parser.add_argument('--logfile', type=str,
                        help='logfile. Will write to stderr if not specified')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    subparsers = parser.add_subparsers()
    for k, h, addp, impl in self.supported_submodules:
      subparser = subparsers.add_parser(k, help=h)
      addp(subparser)
      subparser.set_defaults(func=impl)
    self.parser = parser
    return parser


def main(argv=None):
  app = App()
  parser = app.make_parser()
  args = parser.parse_args(argv)
  log_level = getattr(logging, args.loglevel)
  kwargs = {'format': LOG_FORMAT, 'datefmt': LOG_DATEFMT, 'level': log_level}
  if args.logfile:
    kwargs['filename'] = args.logfile
  logging.basicConfig(**kwargs)
  logger = logging.getLogger()
  args.func(logger, args)
