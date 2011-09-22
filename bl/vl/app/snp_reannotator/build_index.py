"""
Build dbSNP index database from Galaxy genome segment extractor output
in interval format.
"""
import argparse, shelve, csv, os
from cPickle import HIGHEST_PROTOCOL as HP


HELP_DOC = __doc__
SYNC_INTERVAL = 10000  # number of input records after which we resync the db



def make_parser(parser):
  parser.add_argument("-i", "--input-file", metavar="FILE", help="input file")
  parser.add_argument("-o", '--output-file', metavar='FILE',
                      help='output index file')


def main(logger, args):
  index = None
  try:
    index = shelve.open(args.output_file, "n", protocol=HP, writeback=True)
    with open(args.input_file) as f:
      bn = os.path.basename(args.input_file)
      logger.info("processing %r" % bn)
      reader = csv.reader(f, delimiter="\t")
      for i, r in enumerate(reader):
        try:
          tag = r[3]
          seq = r[-1].upper()
        except IndexError:
          msg = "%r: bad input format, bailing out" % bn
          logger.critical(msg)
          raise ValueError(msg)
        else:
          index.setdefault(seq, []).append(tag)
          if (i+1) % SYNC_INTERVAL == 0:
            logger.info("processed %d records: syncing db" % (i+1))
            index.sync()
      logger.info("processed %d records overall" % (i+1))
  finally:
    if index:
      index.close()


def do_register(registration_list):
  registration_list.append(('build_index', HELP_DOC, make_parser, main))
