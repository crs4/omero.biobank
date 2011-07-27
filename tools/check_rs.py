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
"""

import sys, csv, re, logging, optparse, shelve, anydbm
logging.basicConfig(level=logging.DEBUG)
from cPickle import HIGHEST_PROTOCOL as HP
from contextlib import closing

from bl.core.seq.io.fasta import RawFastaReader
from bl.core.seq.utils import reverse_complement
from bl.core.utils.null_logger import NullLogger


class FlankTooShortError(Exception): pass
class BadSnpPosError(Exception): pass
class BadDbSnpHeader(Exception): pass


class SnpAnnReader(csv.DictReader):
  """
  Reads tab-separated files whose header has at least the following fields::

    label rs_label mask

  Where label is the chip manufacturer's label, rs_label is the
  putative rs label (None if absent) and mask is the sequence in the
  LEFT_FLANK[ALLELE_A/ALLELE_B]RIGHT_FLANK format.
  """
  def __init__(self, f, logger=None):
    csv.DictReader.__init__(self, f, delimiter="\t", quoting=csv.QUOTE_NONE)
    self.logger = logger or NullLogger()

  def next(self):
    r = csv.DictReader.next(self)
    label = r['label']
    rs_label = None if r['rs_label'] == 'None' else r['rs_label']
    m = re.match(r'^([A-Z]+)\[([^/]+)/([^\]]+)\]([A-Z]+)$', r['mask'],
                 flags=re.IGNORECASE)
    if m:
      lflank, alleles, rflank = (m.group(1), (m.group(2), m.group(3)),
                                 m.group(4))
      return label, rs_label, lflank, rflank, alleles
    else:
      self.logger.error("%r: bad mask format: %r -- skipping"
                        % (label, r['mask']))
      return self.next()


class DbSnpReader(RawFastaReader):

  def __init__(self, f, offset=0, split_size=None, logger=None):
    super(DbSnpReader, self).__init__(f, offset, split_size)
    self.logger = logger or NullLogger()

  def next(self):
    self.header, self.seq = super(DbSnpReader, self).next()
    try:
      self.rs_id, self.pos, alleles = self.__parse_header()
    except BadDbSnpHeader:
      self.logger.error("bad header %r" % (self.header))
      return self.next()      
    try:
      left_flank, right_flank = self.__parse_seq()
    except BadSnpPosError:
      self.logger.error("%r: seq[%d] does not exist -- skipping"
                        % (self.rs_id, self.pos))
      return self.next()
    return self.rs_id, left_flank, alleles, right_flank

  def __parse_header(self):
    try:
      rs_id = re.search(r'rs\d+', self.header).group()
      pos = int(re.search(r'pos\s*=\s*(\d+)', self.header).groups()[0]) - 1
      alleles = re.search(r'alleles\s*=\s*"([^"]+)', self.header).groups()[0]
    except AttributeError:
      raise BadDbSnpHeader
    return rs_id, pos, alleles

  def __parse_seq(self):
    seq = self.seq.replace(" ", "").upper()
    try:
      snp = seq[self.pos]
    except IndexError:
      raise BadSnpPosError
    if snp in "ACGT":
      self.logger.warn("%r: seq[%d] has unexpected value %r"
                       % (self.rs_id, self.pos, seq[self.pos]))
    return seq[:self.pos], seq[self.pos+1:]


def mask_to_key(left_flank, right_flank, N):
  if len(left_flank) < N or len(right_flank) < N:
    raise FlankTooShortError
  seq = left_flank[-N:] + right_flank[:N]
  return min(seq, reverse_complement(seq))


def update_index(db_snp_reader, N, index=None, logger=None):
  logger = logger or NullLogger()
  if index is None:
    index = {}
  for rs_label, lflank, alleles, rflank in db_snp_reader:
    try:
      key = mask_to_key(lflank, rflank, N)
    except FlankTooShortError:
      logger.warn("%r: flank(s) too short, NOT adding to index" % rs_label)
    else:
      index.setdefault(key, []).append(rs_label)
  return index


def check_rs(ann_table, index, N, logger=None):
  logger = logger or NullLogger()
  check_map = {}
  for label, rs_label, lflank, rflank in ann_table:
    try:
      k = mask_to_key(lflank, rflank, N)
    except FlankTooShortError:
      logger.warn("%r: flank(s) too short, forcing a no-match" % label)
      true_rs_labels = ["None"]
    else:
      true_rs_labels = index.get(k, ["None"])
    check_map[label] = [rs_label, true_rs_labels, rs_label in true_rs_labels]
  return check_map


def get_rs_to_label(check_map):
  rs_to_label = {}
  for label, (_, true_rs_labels, _) in check_map.iteritems():
    for tl in true_rs_labels:
      rs_to_label[tl] = label
  return rs_to_label


def get_true_seqs(db_snp_reader, M, rs_to_label):
  for rs_label, lflank, alleles, rflank in db_snp_reader:
    try:
      label = rs_to_label[rs_label]
    except KeyError:
      continue
    else:
      true_seq = "%s[%s]%s" % (lflank[-M:], alleles, rflank[:M])
      yield label, true_seq


def update_check_map(db_snp_reader, check_map, M, rs_to_label):
  for label, true_seq in get_true_seqs(db_snp_reader, M, rs_to_label):
    try:
      current_true_seq = check_map[label][3]
    except IndexError:
      check_map[label].append(true_seq)
    else:
      if len(true_seq) > len(current_true_seq):
        check_map[label][3] = true_seq


def write_check_map(check_map, outf):
  outf.write("label\trs_label\ttrue_rs_labels\tcheck\ttrue_seq\n")
  for label, data in check_map.iteritems():
    if len(data) < 4:
      data.append("None")
    data[1] = ",".join(data[1])
    data[2] = repr(data[2])
    outf.write("%s\t%s\n" % (label, "\t".join(data)))


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
        update_index(db_snp_reader, opt.flank_cut_size, index, logger)
      logger.info("syncing index")
      index.sync()
  else:
    logger.info("using existing index at %r" % index_fn)
  finally:
    logger.info("n. keys in index: %d" % len(index))
    index.close()

  logger.info("checking rs labels")
  with closing(shelve.open(index_fn, "r")) as index:
    check_map = check_rs(ann_table, index, opt.flank_cut_size, logger)
  del ann_table
  logger.info("n. keys in check_map: %d" % len(check_map))

  logger.info("building rs_to_label")
  rs_to_label = get_rs_to_label(check_map)
  logger.info("n. keys in rs_to_label: %d" % len(rs_to_label))
  
  logger.info("getting true sequences")
  logger.info("output flank cut size: %d" % opt.out_flank_cut_size)
  for fn in db_filenames:
    logger.info("processing %r" % fn)
    with open(fn) as f:
      db_snp_reader = DbSnpReader(f, logger=logger)
      update_check_map(db_snp_reader, check_map, opt.out_flank_cut_size,
                       rs_to_label)
  logger.info("writing output to %r" % opt.output_file.name)
  write_check_map(check_map, opt.output_file)
  
  if opt.output_file is not sys.stdout:
    opt.output_file.close()


if __name__ == "__main__":
  main(sys.argv)
