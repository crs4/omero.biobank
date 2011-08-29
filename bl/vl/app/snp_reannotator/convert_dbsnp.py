"""
Convert dbSNP files to the format expected by SEAL's realign_snp tool.
"""
import argparse


HELP_DOC = __doc__


def make_parser(parser):
  parser.add_argument('--dbsnp-dir', metavar='DIR', required=True,
                      help='a directory containing dbSNP files')
  parser.add_argument('--output-file', metavar='FILE', required=True,
                      help='output file')


def main(logger, args):
  print "This is a test for convert_dbsnp"
  print "  logger: %r" % logger
  print "  args: %r" % args


def do_register(registration_list):
  registration_list.append(('convert_dbsnp', HELP_DOC, make_parser, main))
