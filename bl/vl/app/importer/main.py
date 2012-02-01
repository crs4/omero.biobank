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

import sys, os, argparse, logging
from importlib import import_module


SUBMOD_NAMES = [
  "study",
  "individual",
  "biosample",
  "titer_plate",
  "device",
  "data_sample",
  "data_object",
  "group",
  "data_collection",
  "marker_definition",
  "marker_alignment",
  "markers_set",
  "diagnosis",
  "enrollment",
  ]
SUBMODULES = [import_module("%s.%s" % (__package__, n)) for n in SUBMOD_NAMES]


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
                        choices=['DEBUG', 'INFO', 'WARNING', 'CRITICAL'],
                        help='logging level', default='INFO')
    parser.add_argument('-i', '--ifile', type=argparse.FileType('r'),
                        help='input tsv file', default=sys.stdin)
    parser.add_argument('-o', '--ofile', type=argparse.FileType('w'),
                        help='output tsv file', default=sys.stdout)
    parser.add_argument('-H', '--host', metavar="STRING",
                        help='OMERO hostname', default='localhost')
    parser.add_argument('-U', '--user', metavar="STRING",
                        help='OMERO user', default='test')
    parser.add_argument('-P', '--passwd', metavar="STRING",
                        help='OMERO password', required=True)
    parser.add_argument('--operator', metavar="STRING",
                        help='operator identifier', required=True)
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
  logformat = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  loglevel  = getattr(logging, args.loglevel)
  if args.logfile:
    logging.basicConfig(filename=args.logfile, format=logformat, level=loglevel)
  else:
    logging.basicConfig(format=logformat, level=loglevel)
  logger = logging.getLogger()
  args.func(logger, args)
