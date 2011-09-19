from bl.vl.utils.snp import split_mask


POSSIBLE_ALLELES = frozenset(['A', 'C', 'G', 'T'])
MARKER_DEF_FIELDS = ("label", "rs_label", "mask")
MARKER_AL_FIELDS = ("marker_vid", "ref_genome", "chromosome", "pos", "strand",
                    "allele", "copies")


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
