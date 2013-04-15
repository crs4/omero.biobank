# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
This application provides a set of tools for managing SNP data.

Its main features are:

* data extraction from chip annotation files
* data conversion to/from formats used by the BWA sequence aligner
"""

import argparse, logging
from importlib import import_module


LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
SUBMOD_NAMES = [
  "convert_dbsnp",
  "convert_affy",
  "convert_ill",
  "markers_to_fastq",
  "convert_sam",
  "patch_alignments",
  "build_index",
  "lookup_index",
  ]
SUBMODULES = [import_module("%s.%s" % (__package__, n)) for n in SUBMOD_NAMES]


class App(object):
  
  def __init__(self):
    self.supported_submodules = []
    for m in SUBMODULES:
      m.do_register(self.supported_submodules)

  def make_parser(self):
    parser = argparse.ArgumentParser(description="Manage SNP data")
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
