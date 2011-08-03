import sys, os

import argparse
import sys

import csv

"""

Import Utility
==============


The purpose of this utility is to import in vl externally generated data.

FIXME documentations should be extracted from imported modules..

"""


#---------------------------------------------------------------
import logging, time
#---------------------------------------------------------------
import bl.vl.app.importer.study
import bl.vl.app.importer.individual
import bl.vl.app.importer.biosample
import bl.vl.app.importer.titer_plate
import bl.vl.app.importer.device
import bl.vl.app.importer.data_sample
import bl.vl.app.importer.data_object
import bl.vl.app.importer.data_collection
import bl.vl.app.importer.marker_definition
import bl.vl.app.importer.marker_alignment
import bl.vl.app.importer.markers_set
import bl.vl.app.importer.diagnosis



class App(object):
  def __init__(self):
    self.supported_submodules = []
    bl.vl.app.importer.study.do_register(self.supported_submodules)
    bl.vl.app.importer.individual.do_register(self.supported_submodules)
    bl.vl.app.importer.biosample.do_register(self.supported_submodules)
    bl.vl.app.importer.titer_plate.do_register(self.supported_submodules)
    bl.vl.app.importer.device.do_register(self.supported_submodules)
    bl.vl.app.importer.data_sample.do_register(self.supported_submodules)
    bl.vl.app.importer.data_object.do_register(self.supported_submodules)
    bl.vl.app.importer.data_collection.do_register(self.supported_submodules)
    bl.vl.app.importer.marker_definition.do_register(self.supported_submodules)
    bl.vl.app.importer.marker_alignment.do_register(self.supported_submodules)
    bl.vl.app.importer.markers_set.do_register(self.supported_submodules)
    bl.vl.app.importer.diagnosis.do_register(self.supported_submodules)

  def make_parser(self):
    parser = argparse.ArgumentParser(description="A magic importer")
    parser.add_argument('--logfile', type=str,
                        help='logfile. Will write to stderr if not specified')
    parser.add_argument('--loglevel', type=str,
                        choices=['DEBUG', 'INFO', 'WARNING', 'CRITICAL'],
                        help='logging level', default='INFO')
    parser.add_argument('-i', '--ifile', type=argparse.FileType('r'),
                        help='the input tsv file',
                        default=sys.stdin)
    parser.add_argument('-o', '--ofile', type=argparse.FileType('w'),
                        help='the output mapping tsv file',
                        default=sys.stdout)
    parser.add_argument('-H', '--host', type=str,
                        help='omero host system',
                        default='localhost')
    parser.add_argument('-U', '--user', type=str,
                        help='omero user',
                        default='root')
    parser.add_argument('-P', '--passwd', type=str,
                        help='omero user passwd',
                        required=True)
    parser.add_argument('--operator', type=str,
                        help='operator identifier',
                        required=True)
    parser.add_argument('-K', '--keep-tokens', type=int,
                        default=1, help='omero tokens for open session')

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
