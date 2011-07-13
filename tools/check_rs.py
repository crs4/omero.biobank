"""
Check the rs annotation of a SNP versus NCBI dbSNP.

dbSNP data is read from fasta dumps downloaded from:
ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606/rs_fasta
"""

import sys, csv, re, logging, optparse
logging.basicConfig(level=logging.DEBUG)
from bl.core.seq.io.fasta import RawFastaReader
from bl.core.seq.utils import reverse_complement


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
    m = re.match('^([ACGT]+)\[([ACGT])/([ACGT])\]([ACGT]+)$', r['mask'],
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
  seq = left_flank[-N:] + right_flank[:N]
  return min(seq, reverse_complement(seq))


def build_ann_table(snp_ann_reader):
  table = []
  max_flank_len = 0
  for label, rs_label, lflank, rflank, alleles in snp_ann_reader:
    table.append((label, rs_label, lflank, rflank))
    M = max(len(lflank), len(rflank))
    if M > max_flank_len:
      max_flank_len = M
  return max_flank_len, table


def update_index(db_snp_reader, N, index=None):
  if index is None:
    index = {}
  for rs_label, lflank, rflank in db_snp_reader:
    key = mask_to_key(lflank, rflank, N)
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
  parser.add_option("-o", "--output-file", metavar="FILE", help="output file")
  parser.add_option("--log-level", metavar="LOG_LEVEL", help="log level")
  return parser


def check_rs(ann_table, index, N, outf=sys.stdout):
  outf.write("label\trs_label\ttrue_rs_labels\tcheck\n")
  for label, rs_label, lflank, rflank in ann_table:
    k = mask_to_key(lflank, rflank, N)
    try:
      true_rs_labels = index[k]
    except KeyError:
      true_rs_labels = [None]
    outf.write("%s\t%s\t%r\t%r\n" % (
      label, rs_label, true_rs_labels, rs_label in true_rs_labels
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
    N, ann_table = build_ann_table(reader)
    logger.info("n. records: %d" % len(ann_table))
    logger.info("max flank len: %d" % N)

  index = {}
  logger.info("building index")
  for fn in db_filenames:
    logger.info("processing %r" % fn)
    with open(fn) as f:
      db_snp_reader = DbSnpReader(f, logger=logger)
      update_index(db_snp_reader, N, index=index)
  logger.info("n. keys in index: %d" % len(index))

  logger.info("checking rs labels")
  check_rs(ann_table, index, N, opt.output_file)
  if opt.output_file is not sys.stdout:
    opt.output_file.close()


if __name__ == "__main__":
  main(sys.argv)
