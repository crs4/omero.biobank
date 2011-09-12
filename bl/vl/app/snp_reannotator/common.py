from bl.vl.utils.snp import split_mask


POSSIBLE_ALLELES = frozenset(['A', 'C', 'G', 'T'])
MARKER_DEF_FIELDS = "label", "rs_label", "mask"


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
