"""
Build dbSNP index database.
"""
import argparse


HELP_DOC = __doc__


def make_parser(parser):
  parser.add_argument("-N", "--flank-cut-size", type=int, metavar="INT",
                      help="cut flanks at this size for mapping purposes",
                      default=16)
  parser.add_argument("-M", "--out-flank-cut-size", type=int, metavar="INT",
                      help="cut output flanks at this size", default=128)
  parser.add_argument('--index-file', metavar='FILE', help='index file')


def main(logger, args):
  print "This is a test for build_index"
  print "  logger: %r" % logger
  print "  args: %r" % args


def do_register(registration_list):
  registration_list.append(('build_index', HELP_DOC, make_parser, main))
