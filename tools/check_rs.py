"""
Check the rs annotation of a SNP versus NCBI dbSNP.

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
up in the index because of the truncation).
"""

import sys, csv, re, logging, optparse
logging.basicConfig(level=logging.DEBUG)
from bl.core.seq.io.fasta import RawFastaReader
from bl.core.seq.utils import reverse_complement


class FlankTooShortError(Exception):
  pass


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
    self.logger = logger

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
      if self.logger:
        self.logger.error("%r: bad mask format: %r -- skipping"
                          % (label, r['mask']))
      return self.next()


class DbSnpReader(RawFastaReader):

  def __init__(self, f, offset=0, split_size=None, logger=None):
    super(DbSnpReader, self).__init__(f, offset, split_size)
    self.logger = logger

  def next(self):
    self.header, self.seq = super(DbSnpReader, self).next()
    self.rs_id, self.pos = self.__parse_header()
    left_flank, right_flank = self.__parse_seq()
    return self.rs_id, left_flank, right_flank

  def __parse_header(self):
    header = self.header.split("|")
    rs_id = header[2].split(" ", 1)[0]
    pos = int(header[3].rsplit("=", 1)[-1]) - 1
    return rs_id, pos

  def __parse_seq(self):
    seq = self.seq.replace(" ", "").upper()
    if seq[self.pos] in "ACGT":
      if self.logger:
        self.logger.warn("%r: seq[%d] has unexpected value %r"
                         % (self.rs_id, self.pos, seq[self.pos]))
    return seq[:self.pos], seq[self.pos+1:]


def mask_to_key(left_flank, right_flank, N):
  if len(left_flank) < N or len(right_flank) < N:
    raise FlankTooShortError
  seq = left_flank[-N:] + right_flank[:N]
  return min(seq, reverse_complement(seq))


def update_index(db_snp_reader, N, index=None, logger=None):
  if index is None:
    index = {}
  for rs_label, lflank, rflank in db_snp_reader:
    try:
      key = mask_to_key(lflank, rflank, N)
    except FlankTooShortError:
      if logger:
        logger.warn("%r: flank(s) too short, NOT adding to index" % rs_label)
    else:
      index.setdefault(key, []).append(rs_label)
  return index


class HelpFormatter(optparse.IndentedHelpFormatter):
  def format_description(self, description):
    return description + "\n" if description else ""


def make_parser():
  parser = optparse.OptionParser(
    usage="%prog [OPTIONS] ANN_FILE DB_FILE [DB_FILE]...",
    formatter=HelpFormatter(),
    )
  parser.add_option("-N", "--flank-cut-size", type="int", metavar="INT",
                    help="cut flanks at this size for mapping purposes")
  parser.add_option("-o", "--output-file", metavar="FILE", help="output file")
  parser.add_option("--log-level", metavar="LOG_LEVEL", help="log level")
  return parser


def check_rs(ann_table, index, N, outf=sys.stdout, logger=None):
  outf.write("label\trs_label\ttrue_rs_labels\tcheck\n")
  for label, rs_label, lflank, rflank in ann_table:
    try:
      k = mask_to_key(lflank, rflank, N)
    except FlankTooShortError:
      if logger:
        logger.warn("%r: flank(s) too short, forcing a no-match" % label)
        true_rs_labels = ["None"]
    else:
      true_rs_labels = index.get(k, ["None"])
    outf.write("%s\t%s\t%s\t%r\n" % (
      label, rs_label, ",".join(true_rs_labels), rs_label in true_rs_labels
      ))


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

  index = {}
  logger.info("building index")
  for fn in db_filenames:
    logger.info("processing %r" % fn)
    with open(fn) as f:
      db_snp_reader = DbSnpReader(f, logger=logger)
      update_index(db_snp_reader, opt.flank_cut_size, index, logger)
  logger.info("n. keys in index: %d" % len(index))

  logger.info("checking rs labels")
  check_rs(ann_table, index, opt.flank_cut_size, opt.output_file, logger)
  if opt.output_file is not sys.stdout:
    opt.output_file.close()


if __name__ == "__main__":
  main(sys.argv)
