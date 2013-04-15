# BEGIN_COPYRIGHT
# END_COPYRIGHT

import csv, os

from bl.core.seq.utils import reverse_complement as rc
import bl.vl.utils.snp as snp_utils


POSSIBLE_ALLELES = frozenset(['A', 'C', 'G', 'T'])
MARKER_DEF_FIELDS = ("label", "mask", "index", "allele_flip")
MARKER_AL_FIELDS = ("marker_vid", "ref_genome", "chromosome", "pos", "strand",
                    "allele", "copies")
DUMMY_AL_VALUES = {
  "chromosome": '0',
  "pos": '0',
  "strand": '-',
  "allele": 'A',
  }
CHR_CODES = {
  "chr1": 1,
  "chr2": 2,
  "chr3": 3,
  "chr4": 4,
  "chr5": 5,
  "chr6": 6,
  "chr7": 7,
  "chr8": 8,
  "chr9": 9,
  "chr10": 10,
  "chr11": 11,
  "chr12": 12,
  "chr13": 13,
  "chr14": 14,
  "chr15": 15,
  "chr16": 16,
  "chr17": 17,
  "chr18": 18,
  "chr19": 19,
  "chr20": 20,
  "chr21": 21,
  "chr22": 22,
  "chrX": 23,
  "chrY": 24,
  "chrXY": 25,  
  "chrM": 26,
  #---
  "chrx": 23,
  "chry": 24,
  "chrxy": 25,
  "chrm": 26,
}


class SeqNameSerializer(object):

  DEFAULT_SEP = "|"

  def __init__(self, sep=DEFAULT_SEP):
    self.sep = sep

  def serialize(self, label, allele_code, snp_offset, alleles):
    return self.sep.join(
      [label, allele_code, str(snp_offset), "".join(alleles)]
      )

  def deserialize(self, seq_name):
    label, allele_code, snp_offset_str, alleles_str = seq_name.split("|")
    return label, allele_code, int(snp_offset_str), tuple(alleles_str)


def check_mask(mask):
  try:
    lflank, alleles, rflank = snp_utils.split_mask(mask)
  except ValueError:
    problem = "bad mask format"
  else:
    if not(2 <= len(alleles) <= 4 and set(alleles) <= POSSIBLE_ALLELES):
      problem = "bad alleles: %r" % list(alleles)
    else:
      problem = None
  return problem


def process_mask(mask, allele_a, allele_b):
  """
  Convert mask to top Illumina format and determine allele flip.

  In biobank, the first and second allele are defined by the central
  part of the mask as stored in the marker set table (in top format,
  if possible). If the manufacturer provides alleles in reversed
  order, we set the allele_flip flag to True, so that SNP calling
  results can be correctly interpreted.

  Input: SNP mask, first and second allele as provided by the manufacturer
  Output: top SNP mask (if convertible), allele flip, problem encountered
  """
  error = ""
  try:
    mask = snp_utils.split_mask(mask)
  except ValueError as e:
    return 'None', False, "%s, setting mask to 'None'" % e
  orig_alleles = mask[1][:]
  if not(len(mask[1]) == 2 and set(mask[1]) <= POSSIBLE_ALLELES):
    return 'None', False, "bad alleles %r, setting mask to 'None'" % (mask[1],)
  try:
    mask = snp_utils.convert_to_top(mask)
  except ValueError as e:
    error = "mask cannot be converted to top"
  else:
    if mask[1] != orig_alleles:
      allele_a, allele_b = rc((allele_a, allele_b))
    if not set(mask[1]) == set((allele_a, allele_b)):
      error = "allele mismatch: %r != (%s, %s)" % (mask[1], allele_a, allele_b)
  return snp_utils.join_mask(mask), mask[1] != (allele_a, allele_b), error


def build_index_key(seq):
  return min(seq, rc(seq))


def write_mdef(stream, fo, header=True):
  """
  Given a stream of [label, mask, index, allele_flip] rows, write a
  tsv file suitable for input to the marker set importer.

  If header is False, field names will not be written in the first row
  (this is useful if you want to generate the output file with
  multiple calls to this function).
  """
  writer = csv.writer(fo, delimiter="\t", lineterminator=os.linesep)
  if header:
    writer.writerow(MARKER_DEF_FIELDS)
  for row in stream:
    writer.writerow(map(str, row))
