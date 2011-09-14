"""

Kb_Query Utility
================

The ``kb_query`` is the basic command line tool that can be used to
extract information from VL. Similarly to the importer tool is
structured around a modular interface with context specific modules::

  usage: kb_query [-h] [--logfile LOGFILE]
                  [--loglevel {DEBUG,INFO,WARNING,CRITICAL}] [-o OFILE]
                  [-H HOST] [-U USER] [-P PASSWD] [-K KEEP_TOKENS] --operator
                  OPERATOR
                  {map_vid,query,global_stats,selector} ...

  A magic kb_query app

  positional arguments:
    {map_vid,query,global_stats,selector}
      map_vid             Map user defined objects label to vid.
      global_stats        Extract global stats from KB in tabular form.
      selector            Select a group of individuals
      query               Select a group of individuals

  optional arguments:
    -h, --help            show this help message and exit
    --logfile LOGFILE     logfile. Will write to stderr if not specified
    --loglevel {DEBUG,INFO,WARNING,CRITICAL}
                          logging level
    -o OFILE, --ofile OFILE
                          the output tsv file
    -H HOST, --host HOST  omero host system
    -U USER, --user USER  omero user
    -P PASSWD, --passwd PASSWD
                          omero user passwd
    -K KEEP_TOKENS, --keep-tokens KEEP_TOKENS
                          omero tokens for open session
    --operator OPERATOR   operator identifier

"""

import sys, os

import argparse
import sys

import csv



#---------------------------------------------------------------
import logging, time
#---------------------------------------------------------------

import bl.vl.app.kb_query.map_vid
import bl.vl.app.kb_query.global_stats
import bl.vl.app.kb_query.selector
import bl.vl.app.kb_query.query
#import bl.vl.app.kb_query.tabular
#import bl.vl.app.kb_query.markers
#import bl.vl.app.kb_query.ehr

class App(object):
  def __init__(self):
    self.supported_submodules = []
    bl.vl.app.kb_query.map_vid.do_register(self.supported_submodules)
    bl.vl.app.kb_query.global_stats.do_register(self.supported_submodules)
    bl.vl.app.kb_query.selector.do_register(self.supported_submodules)
    bl.vl.app.kb_query.query.do_register(self.supported_submodules)
    #bl.vl.app.kb_query.tabular.do_register(self.supported_submodules)
    #bl.vl.app.kb_query.markers.do_register(self.supported_submodules)
    #bl.vl.app.kb_query.ehr.do_register(self.supported_submodules)

  def make_parser(self):
    parser = argparse.ArgumentParser(description="A magic kb_query app")
    parser.add_argument('--logfile', type=str,
                        help='logfile. Will write to stderr if not specified')
    parser.add_argument('--loglevel', type=str,
                        choices=['DEBUG', 'INFO', 'WARNING', 'CRITICAL'],
                        help='logging level', default='INFO')
    parser.add_argument('-o', '--ofile', type=argparse.FileType('w'),
                        help='the output tsv file',
                        default=sys.stdout)
    parser.add_argument('-H', '--host', type=str,
                        help='omero host system',
                        default='localhost')
    parser.add_argument('-U', '--user', type=str,
                        help='omero user',
                        default='root')
    parser.add_argument('-P', '--passwd', type=str,
                        help='omero user passwd')
    parser.add_argument('-K', '--keep-tokens', type=int,
                        default=1, help='omero tokens for open session')
    parser.add_argument('--operator', type=str,
                        help='operator identifier',
                        required=True)


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

