# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
The purpose of this tool is to import externally generated data into
the KB. Import operations can almost always be described as:

  * reading a tsv input file with one column named **label**, a unique
    id for the specific object defined and other fields. When the
    specific object is linked to another, e.g., a blood sample to an
    individual, there will be one column named **source** with the VID
    (the unique id assigned by the KB to the object) of an object to
    which this object should be linked to;

  * saving data read from the tsv file to the KB, plus other data
    provided as parameters. For each input record, the program
    generates the corresponding object as well as any relevant action
    that links it to its source;

  * outputting an object mapping tsv file with four columns: study,
    label, object_type and VID, where object_type is the KB object type.

The user is responsible for mapping labels to VIDs: in most cases you
should be able to use ``kb_query map_vid`` for this.
"""

import sys, argparse, logging, os
from importlib import import_module
import bl.vl.kb.drivers.omero.utils as vlu

SUBMOD_NAMES = [
  "study",
  "individual",
  "biosample",
  "samples_container",
  "device",
  "data_sample",
  "data_object",
  "group",
  "data_collection",
  "vessels_collection",
  "marker_alignment",
  "markers_set",
  "diagnosis",
  "enrollment",
  "birth_data",
  "laneslot",
  "sequencing_data_sample",
  ]
SUBMODULES = [import_module("%s.%s" % (__package__, n)) for n in SUBMOD_NAMES]

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

class App(object):
  
  def __init__(self):
    self.supported_submodules = []
    for m in SUBMODULES:
      m.do_register(self.supported_submodules)

  def make_parser(self):
    parser = argparse.ArgumentParser(description="KB importer")
    parser.add_argument('--logfile', metavar="FILE",
                        help='log file, defaults to stderr')
    parser.add_argument('--loglevel', metavar="STRING",
                        choices=LOG_LEVELS, help='logging level',
                        default='INFO')
    parser.add_argument('-i', '--ifile', type=argparse.FileType('r'),
                        help='input tsv file', default=sys.stdin)
    parser.add_argument('-o', '--ofile', type=argparse.FileType('w'),
                        help='output tsv file', default=sys.stdout)
    parser.add_argument('-r', '--report_file', type=argparse.FileType('w'),
                        help='report tsv file', default=sys.stdout)
    parser.add_argument('-H', '--host', metavar="STRING",
                        help='OMERO hostname')
    parser.add_argument('-U', '--user', metavar="STRING",
                        help='OMERO user')
    parser.add_argument('-P', '--passwd', metavar="STRING",
                        help='OMERO password')
    parser.add_argument('--operator', metavar="STRING",
                        help='operator identifier', required=True)
    parser.add_argument('--blocking-validator', action='store_true',
                        help='block import if at least one record doesn\'t pass data validation')
    parser.add_argument('-K', '--keep-tokens', type=int,
                        default=1, help='OMERO tokens for open session')
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
  loglevel  = getattr(logging, args.loglevel)
  kwargs = {'format'  : LOG_FORMAT,
            'datefmt' : LOG_DATEFMT,
            'level'   : loglevel}
  if args.logfile:
    kwargs['filename'] = args.logfile
  logging.basicConfig(**kwargs)
  logger = logging.getLogger()
  try:
    host = args.host or vlu.ome_host()
    user = args.user or vlu.ome_user()
    passwd = args.passwd or vlu.ome_passwd()
  except ValueError, ve:
    logger.critical(ve)
    sys.exit(ve)
  args.func(logger, host, user, passwd, args)
