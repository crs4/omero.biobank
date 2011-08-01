"""
Check chip manufacturer's marker annotations against NCBI dbSNP.

dbSNP data is read from fasta dumps downloaded from:
ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606/rs_fasta

NOTE: this tool does not deal with the trailing 'comment':

# ================ 
# File created at: 
# `date` 
# ================

found in original files downloaded from NCBI. Such 'comments' are not
legal in FASTA files. This means that, with no pre-processing, those
lines are included in the last sequence (however, they might not end
up in the index because of flank truncation).

WARNING: indexing the full dbSNP can take several hours, with a peak
memory usage of about 2 GB.
"""

import sys, csv, re, logging, optparse, shelve, anydbm
logging.basicConfig(
  level=logging.DEBUG,
  format='%(asctime)s|%(levelname)-8s|%(message)s',
  datefmt='%Y-%m-%d %H:%M:%S',
  )
from cPickle import HIGHEST_PROTOCOL as HP
from contextlib import closing

from bl.core.seq.io import DbSnpReader
from bl.core.seq.utils import reverse_complement
from bl.core.utils import NullLogger, longest_subs


class FlankTooShortError(Exception): pass


class SnpAnnReader(csv.DictReader):
  """
  Reads tab-separated files whose header has at least the following fields::

    label rs_label mask

  Where label is the chip manufacturer's label, rs_label is the
  putative rs label (None if absent) and mask is the sequence in the
  LEFT_FLANK[ALLELE_A/ALLELE_B]RIGHT_FLANK format.
  """
  
  MASK_PATTERN = re.compile(r'^([A-Z]+)\[([^/]+)/([^\]]+)\]([A-Z]+)$',
                            re.IGNORECASE)
  
  def __init__(self, f, logger=None):
    csv.DictReader.__init__(self, f, delimiter="\t", quoting=csv.QUOTE_NONE)
    self.logger = logger or NullLogger()

  def next(self):
    r = csv.DictReader.next(self)
    label = r['label']
    rs_label = None if r['rs_label'] == 'None' else r['rs_label']
    m = self.MASK_PATTERN.match(r['mask'])
    try:
      lflank, a1, a2, rflank = m.groups()
    except AttributeError:
      self.logger.error("%r: bad mask format: %r -- skipping"
                        % (label, r['mask']))
      return self.next()
    return label, rs_label, lflank, rflank, (a1, a2)


def get_consensus(masks):
  lflanks, alleles, rflanks = zip(*masks)
  if len(set(alleles)) > 1:
    return None
  return longest_subs(lflanks, reverse=True), alleles[0], longest_subs(rflanks)


def mask_to_key(left_flank, right_flank, N):
  if len(left_flank) < N or len(right_flank) < N:
    raise FlankTooShortError
  seq = left_flank[-N:] + right_flank[:N]
  return min(seq, reverse_complement(seq))


def update_index(index, db_snp_reader, N, M, logger=None):
  logger = logger or NullLogger()
  for rs_label, lflank, alleles, rflank in db_snp_reader:
    try:
      key = mask_to_key(lflank, rflank, N)
    except FlankTooShortError:
      logger.warn("%r: flank(s) too short, NOT adding to index" % rs_label)
    else:
      true_seq = (lflank[-M:], alleles, rflank[:M])
      index.setdefault(key, []).append((rs_label, true_seq))
  return index


def check_rs(ann_table, index, N, outf, logger=None):
  outf.write("label\trs_label\ttrue_rs_labels\tcheck\ttrue_seq\n")
  logger = logger or NullLogger()
  check_map = {}
  for label, rs_label, lflank, rflank in ann_table:
    outf.write("%s\t%s\t" % (label, rs_label))
    try:
      k = mask_to_key(lflank, rflank, N)
    except FlankTooShortError:
      logger.warn("%r: flank(s) too short, forcing a no-match" % label)
      v = None
    else:
      v = index.get(k)
    if v is None:
      outf.write("None\tFalse\tNone")
    else:
      true_rs_labels, true_seqs = zip(*v)
      check = rs_label in true_rs_labels
      if len(true_seqs) == 1:
        out_seq = true_seqs[0]
      else:
        out_seq = get_consensus(true_seqs)
      if out_seq is None:
        logger.warn("%r: inconsistent multiple true masks" % label)
        out_seq = "None"
      else:
        out_seq = "%s[%s]%s" % out_seq
      out_rs_label = rs_label if check else true_rs_labels[0]
      outf.write("%s\t%s\t%s" % (out_rs_label, check, out_seq))
    outf.write("\n")


class HelpFormatter(optparse.IndentedHelpFormatter):
  def format_description(self, description):
    return description + "\n" if description else ""


def make_parser():
  parser = optparse.OptionParser(
    usage="%prog [OPTIONS] ANN_FILE DB_FILE [DB_FILE]...",
    formatter=HelpFormatter(),
    )
  parser.set_description(__doc__.lstrip())
  parser.add_option("-N", "--flank-cut-size", type="int", metavar="INT",
                    help="cut flanks at this size for mapping purposes")
  parser.add_option("-M", "--out-flank-cut-size", type="int", metavar="INT",
                    help="cut output flanks at this size", default=128)
  parser.add_option("-o", "--output-file", metavar="FILE", help="output file")
  parser.add_option("--log-level", metavar="LOG_LEVEL", help="log level",
                    default="WARNING")
  return parser


def main(argv):
  parser = make_parser()
  opt, args = parser.parse_args(argv)
  if len(args) < 3:
    parser.print_help()
    sys.exit(2)
  ann_fn = args[1]
  db_filenames = args[2:]
  try:
    opt.log_level = getattr(logging, opt.log_level)
  except AttributeError:
    sys.exit("No such log level: %r" % opt.log_level)
  logger = logging.getLogger("main")
  logger.setLevel(opt.log_level)
  if opt.output_file:
    opt.output_file = open(opt.output_file, "w")
  else:
    opt.output_file = sys.stdout

  with open(ann_fn) as f:
    reader = SnpAnnReader(f, logger)
    logger.info("processing %r" % ann_fn)
    ann_table = [t[:-1] for t in reader]
  if not opt.flank_cut_size:
    opt.flank_cut_size = min(min(len(lflank), len(rflank))
                             for _, _, lflank, rflank in ann_table)
  logger.info("n. records: %d" % len(ann_table))
  logger.info("flank cut size: %d" % opt.flank_cut_size)
  logger.info("output flank cut size: %d" % opt.out_flank_cut_size)
  
  index_fn = "dbsnp_index_%d_%d" % (opt.flank_cut_size, opt.out_flank_cut_size)
  index = None
  try:
    index = shelve.open(index_fn, "r")
  except anydbm.error:
    logger.info("building index on %r" % index_fn)
    index = shelve.open(index_fn, protocol=HP, writeback=True)
    for fn in db_filenames:
      logger.info("processing %r" % fn)
      with open(fn) as f:
        db_snp_reader = DbSnpReader(f, logger=logger)
        update_index(index, db_snp_reader, opt.flank_cut_size,
                     opt.out_flank_cut_size, logger=logger)
      logger.info("syncing index")
      index.sync()
  else:
    logger.info("using existing index at %r" % index_fn)
  finally:
    logger.info("n. keys in index: %d" % len(index))
    index.close()

  logger.info("writing output to %r" % opt.output_file.name)
  with closing(shelve.open(index_fn, "r")) as index:
    check_rs(ann_table, index, opt.flank_cut_size, opt.output_file, logger)
  
  if opt.output_file is not sys.stdout:
    opt.output_file.close()


if __name__ == "__main__":
  main(sys.argv)
