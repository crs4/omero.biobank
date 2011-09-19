from bl.vl.utils.snp import split_mask


POSSIBLE_ALLELES = frozenset(['A', 'C', 'G', 'T'])
MARKER_DEF_FIELDS = ("label", "rs_label", "mask")
MARKER_AL_FIELDS = ("marker_vid", "ref_genome", "chromosome", "pos", "strand",
                    "allele", "copies")


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
  "chrM": 25,
  #---
  "chrx": 23,
  "chry": 24,
  "chrm": 25,
}


class SeqNameSerializer(object):

  DEFAULT_SEP = "|"

  def __init__(self, sep=DEFAULT_SEP):
    self.sep = sep

  def serialize(self, label, allele, snp_offset):
    return self.sep.join([label, allele, str(snp_offset)])

  def deserialize(self, seq_name):
    label, allele, snp_offset_str = seq_name.split("|")
    return label, allele, int(snp_offset_str)


def check_mask(mask):
  try:
    lflank, alleles, rflank = split_mask(mask)
  except ValueError:
    problem = "bad mask format"
  else:
    if not(2 <= len(alleles) <= 4 and set(alleles) <= POSSIBLE_ALLELES):
      problem = "bad alleles: %r" % list(alleles)
    else:
      problem = None
  return problem
