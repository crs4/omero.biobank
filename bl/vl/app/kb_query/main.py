import sys, os

import argparse
import sys

import csv

"""

Kb_Query Utility
===============


The purpose of this utility is to simplify extracting standard
information from the KB.

For instance, in the example below, we are producing a new file
'''mapped.tsv''' by replacing the column '''individual_label''' with a
new column '''source''' that will contain the vid of the object of
type '''Individual''' uniquely individuated by '''individual_label'''
and '''study'''.

.. code-block:: bash

   kb_query -H biobank05  -o bs_mapped.tsv map \
                          -i blood_sample.tsv \
                          --column 'individual_label' --study BSTUDY \
                          --source-type Individual

"""


#---------------------------------------------------------------
import logging, time
#---------------------------------------------------------------

import bl.vl.app.kb_query.map
#import bl.vl.app.kb_query.tabular
#import bl.vl.app.kb_query.markers
#import bl.vl.app.kb_query.ehr

class App(object):
  def __init__(self):
    self.supported_submodules = []
    bl.vl.app.kb_query.map.do_register(self.supported_submodules)
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

