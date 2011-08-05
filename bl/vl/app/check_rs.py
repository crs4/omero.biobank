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
from contextlib import closing, nested

from bl.core.seq.io import DbSnpReader
from bl.core.seq.utils import reverse_complement as rc
from bl.core.utils import NullLogger, longest_subs
from bl.vl.utils.snp import convert_to_top, split_mask, join_mask, rc_mask


class MaskTooShortError(Exception): pass


class SnpAnnReader(csv.DictReader):
  """
  Reads tab-separated files whose header has at least the following fields::

    label rs_label mask

  Where label is the chip manufacturer's label, rs_label is the
  putative rs label ('None' if absent) and mask is the sequence in the
  LEFT_FLANK[SLASH_SEPARATED_ALLELES]RIGHT_FLANK format.
  """
  def __init__(self, f, logger=None):
    csv.DictReader.__init__(self, f, delimiter="\t", quoting=csv.QUOTE_NONE)
    self.logger = logger or NullLogger()

  def next(self):
    r = csv.DictReader.next(self)
    label = r['label']
    rs_label = None if r['rs_label'] == 'None' else r['rs_label']
    try:
      mask = convert_to_top(split_mask(r['mask']))
    except ValueError, e:
      self.logger.warn("%r: %s, skipping" % (label, e))
      return self.next()
    return label, rs_label, mask


def mask_to_key(mask, N, sep="|"):
  """
  Convert a (left_flank, alleles_tuple, right_flank) mask to a key for
  the dbSNP index.

  FIXME: the problem with this version is that left flanks must be
  compared from the SNP backwards. Here is an example where similar
  masks generate different keys::

    >>> def m2k(m): return mask_to_key(split_mask(m), 4)
    ... 
    >>> s1 = 'AAA[A/T]CTTTT'
    >>> s2 = 'CGTAGCTAAAAAG[A/T]TTTCACTGATCAC'
    >>> print "         ", s1; print join_mask(rc_mask(split_mask(s2)))
              AAA[A/T]CTTTT
    GTGATCAGTGAAA[A/T]CTTTTTAGCTACG
    >>> print m2k(s1); print m2k(s2)
    AAA|CTTTT
    AAAG|TTTC
  """
  lflank, _, rflank = min(mask, rc_mask(mask))
  L, R = len(lflank), len(rflank)
  if L + R < 2*N:
    raise MaskTooShortError
  if L < N:
    R = 2*N - L
  elif R < N:
    L = 2*N - R
  else:
    L = R = N
  return sep.join((lflank[-L:], rflank[:R]))    


def mask_to_key_2(mask, N, sep="|"):
  """
  Convert a (left_flank, alleles_tuple, right_flank) mask to a key for
  the dbSNP index.

  FIXME: better than the other version, in the sense that keys are
  equal around the pipe::

    AAA|CTTTT
    GAAA|CTTT

  They are not good for a lookup though. Converting the mask to top
  yields the same result, but with the additional problem of failing
  for undecidable strands.
  """
  lflank = min(mask[0][::-1], rc(mask[2][::-1]))[::-1]
  rflank = min(mask[2], rc(mask[0]))
  L, R = len(lflank), len(rflank)
  if L + R < 2*N:
    raise MaskTooShortError
  if L < N:
    R = 2*N - L
  elif R < N:
    L = 2*N - R
  else:
    L = R = N
  return sep.join((lflank[-L:], rflank[:R]))    


def update_index(index, db_snp_reader, N, M, logger=None):
  """
  FIXME: convert_to_top: fallback to lexicographically smaller, but
  see problems reported in mask_to_key.
  """
  logger = logger or NullLogger()
  for rs_label, lflank, alleles, rflank in db_snp_reader:
    alleles = tuple(alleles.split("/"))
    ## if len(alleles) != 2:
    ##   logger.warn("%r: bad alleles %r, skipping" % (rs_label, alleles))
    ##   continue
    try:
      lflank, alleles, rflank = convert_to_top((lflank, alleles, rflank))
    except ValueError, e:
      logger.warn("%r: %s, skipping" % (rs_label, e))
      continue
    try:
      key = mask_to_key((lflank, alleles, rflank), N)
    except MaskTooShortError:
      logger.warn("%r: mask too short, skipping" % rs_label)
      continue
    true_seq = (lflank[-M:], alleles, rflank[:M])
    index.setdefault(key, []).append((rs_label, true_seq))
  return index


def check_rs(ann_f, index, N, outf, logger=None):
  """
  FIXME: no consensus. hard cuts to size 32 must be the same; then we
  can report problems if alleles are not equal. Convert to top mask
  and fallback to lexicographically smaller, but see problems reported
  in mask_to_key.

  outcome:
    untouched: match with a single, same rs
    renamed: match with a single, different rs
    problematic:
      match has >2 alleles
      multiple matches
  """
  logger = logger or NullLogger()
  check_map = {}
  n_records = sum(1 for _ in ann_f) - 1
  feedback_step = n_records / 10
  ann_f.seek(0)
  logger.info("n. records: %d" % n_records)
  reader = SnpAnnReader(ann_f, logger)
  outf.write("label\trs_label\ttrue_rs_labels\tcheck\ttrue_seq\n")
  for i, (label, rs_label, mask) in enumerate(reader):
    outf.write("%s\t%s\t" % (label, rs_label))
    try:
      k = mask_to_key(mask, N)
    except MaskTooShortError:
      logger.warn("%r: mask too short, forcing a no-match" % label)
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
        out_seq = join_mask(out_seq)
      out_rs_label = ",".join(true_rs_labels)
      outf.write("%s\t%s\t%s" % (out_rs_label, check, out_seq))
    outf.write("\n")
    if i % feedback_step == 0:
      logger.info("%6.2f %% complete" % (100.*i/n_records))


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
                    help="cut flanks at this size for mapping purposes",
                    default=16)
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

  logger.info("flank cut size: %d" % opt.flank_cut_size)
  logger.info("output flank cut size: %d" % opt.out_flank_cut_size)
  logger.info("output file: %r" % opt.output_file.name)
  
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
    index.close()

  logger.info("reannotating probesets")
  with nested(open(ann_fn), closing(shelve.open(index_fn, "r"))) as (f, index):
    check_rs(f, index, opt.flank_cut_size, opt.output_file, logger)
  
  if opt.output_file is not sys.stdout:
    opt.output_file.close()


if __name__ == "__main__":
  main(sys.argv)
