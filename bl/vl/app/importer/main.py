"""
Import Utility
==============

The purpose of this utility is to import in vl externally generated data.

The import operations can almost always be descrived as:

  * reading a tsv input file with one column named '''label''', an
    unique id for the specific object defined, and other fields, when
    the specific object is linked to another, e.g., a blood sample to
    an individual, there will be one column named '''source''' with the
    vid of an object to which this object should be linked to;

  * saving the information there contained, plus other data provided
    as parameters, the record and the context should provide enough
    information to be able to generate, together with the object, also
    the relavant action linking the saved object to the source
    specified in the '''source''' column;

  * outputting an object mapping tsv file with four columns,
    '''study''', '''label''', '''object_type''', '''vid''', where vid
    is the unique VL id assigned by Omero/VL to the object, and
    object_type is Omero/VL object type of the object.

The mapping from user known source labels to vid is the user
responsibility, the typical procedure is to use utility tools such as
FIXME to query the KB and obtain the needed vid.

The command is structured around a modular interface with context
specific modules::

    usage: importer [-h] [--logfile LOGFILE]
                    [--loglevel {DEBUG,INFO,WARNING,CRITICAL}] [-i IFILE]
                    [-o OFILE] [-H HOST] [-U USER] -P PASSWD --operator OPERATOR
                    [-K KEEP_TOKENS]

                    {data_collection,data_object,data_sample,marker_definition,titer_plate,study,marker_alignment,individual,biosample,diagnosis,markers_set,device,group}
                    ...

    A magic importer

    positional arguments:
      {data_collection,data_object,data_sample,marker_definition,titer_plate,study,marker_alignment,individual,biosample,diagnosis,markers_set,device,group}
        study               import new Study definitions into a virgil system.
        individual          import new individual definitions into a virgil system
                            and register them to a study.
        biosample           import new biosample definitions into a virgil system
                            and link them to previously registered objects.
        titer_plate         import new TiterPlate definitions into a virgil
                            system.
        device              import new Device definitions into a virgil system.
        data_sample         import new data sample definitions into a virgil
                            system and attach them to previously registered
                            samples.
        data_object         import new data object definitions into a virgil
                            system and attach them to previously registered data
                            samples.
        data_collection     import a new data collection definition into a virgil
                            system.
        group               create a new group definition into a virgil system.
        marker_definition   import new marker definitions into VL.
        marker_alignment    import new markers alignments into VL.
        markers_set         import new markers set definition into VL.
        diagnosis           import new diagnosis into VL.

    optional arguments:
      -h, --help            show this help message and exit
      --logfile LOGFILE     logfile. Will write to stderr if not specified
      --loglevel {DEBUG,INFO,WARNING,CRITICAL}
                            logging level
      -i IFILE, --ifile IFILE
                            the input tsv file
      -o OFILE, --ofile OFILE
                            the output mapping tsv file
      -H HOST, --host HOST  omero host system
      -U USER, --user USER  omero user
      -P PASSWD, --passwd PASSWD
                            omero user passwd
      --operator OPERATOR   operator identifier
      -K KEEP_TOKENS, --keep-tokens KEEP_TOKENS
                            omero tokens for open session


"""


import sys, os

import argparse
import sys

import csv



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
import bl.vl.app.importer.group
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
    bl.vl.app.importer.group.do_register(self.supported_submodules)
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
                        default='test')
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
