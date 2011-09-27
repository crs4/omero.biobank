"""
Build dbSNP index database from Galaxy genome segment extractor output
in interval format.
"""
import argparse, shelve, csv, os, tempfile
from cPickle import HIGHEST_PROTOCOL as HP


HELP_DOC = __doc__
SYNC_INTERVAL = 10000  # number of input records after which we resync the db


def make_parser(parser):
  parser.add_argument("-i", "--input-file", metavar="FILE", help="input file")
  parser.add_argument("-o", '--output-dir', metavar='DIR',
                      help='directory where the index file will be written')
  parser.add_argument('--reftag', metavar='STRING', required=True,
                      help='reference genome tag')


def main(logger, args):
  index = None
  fd, temp_index_fn = tempfile.mkstemp(dir=args.output_dir)
  os.close(fd)
  os.remove(temp_index_fn)
  try:
    index = shelve.open(temp_index_fn, "n", protocol=HP, writeback=True)
    with open(args.input_file) as f:
      bn = os.path.basename(args.input_file)
      logger.info("processing %r" % bn)
      reader = csv.reader(f, delimiter="\t")
      first_seq_len = None
      for i, r in enumerate(reader):
        try:
          tag = r[3]
          seq = r[-1].upper()
          if first_seq_len is None:
            first_seq_len = len(seq)
          elif len(seq) != first_seq_len:
            msg = "found input sequences of different length"
            logger.critical(msg)
            raise ValueError(msg)
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
      final_output_fn = os.path.join(
        args.output_dir, "dbsnp_index_%s_%d.db" % (args.reftag, first_seq_len)
        )
      os.rename(temp_index_fn, final_output_fn)
      logger.info("index stored to: %s" % final_output_fn)


def do_register(registration_list):
  registration_list.append(('build_index', HELP_DOC, make_parser, main))
