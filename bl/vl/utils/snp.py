import re
import itertools as it

from bl.core.seq.utils import reverse_complement as rc


MASK_PATTERN = re.compile(r'^([A-Z]+)\[([^/]+)/([^\]]+)\]([A-Z]+)$',
                          re.IGNORECASE)

UNAMBIGUOUS_PAIRS = frozenset([
  frozenset(['A', 'C']),
  frozenset(['A', 'G']),
  frozenset(['C', 'T']),
  frozenset(['G', 'T']),
  ])


def split_mask(mask):
  m = MASK_PATTERN.match(mask)
  try:
    lflank, allele_a, allele_b, rflank = m.groups()
  except AttributeError:
    raise ValueError("bad mask format: %r" % mask)
  else:
    return (lflank, (allele_a, allele_b), rflank)


def join_mask(mask):
  try:
    return '%s[%s/%s]%s' % (mask[0], mask[1][0], mask[1][1], mask[2])
  except IndexError:
    raise ValueError("bad mask format: %r" % (mask,))


def _identify_strand(lflank, alleles, rflank):
  """
  Perform strand designation according to the Illumina convention.

  NOTE: expects all parameters to be uppercase.
  """
  if set(alleles) in UNAMBIGUOUS_PAIRS:
    strand = 'TOP' if 'A' in alleles else 'BOT'
  else:
    for p in it.izip(reversed(lflank), rflank):
      if set(p) in UNAMBIGUOUS_PAIRS:
        strand = 'TOP' if p[0] in 'AT' else 'BOT'
        break
    else:
      raise ValueError('Cannot decide strand of %s' %
                       join_mask((lflank, alleles, rflank)))
  return strand


def convert_to_top(mask, toupper=True):
  """
  Convert a mask with format LeftFlank[AlleleA/AlleleB]RightFlank to
  a new mask obtained by mapping mask to the TOP (following Illumina
  conventions) strand.

  The mask parameter can be a string (e.g., 'AC[A/G]GT') or a list in
  the format output by split_mask (e.g., ['AC', ['A', 'G'], 'GT']);
  the returned mask has the same type as the input one.

  See the following reference for algorithm description:
  
  Illumina, Inc., "TOP/BOT" Strand and "A/B" Allele, technical note, 2006.
  """
  if isinstance(mask, basestring):
    lflank, alleles, rflank = split_mask(mask)
    rebuild_str = True
  else:
    lflank, alleles, rflank = mask
    rebuild_str = False
  alleles = tuple(sorted(alleles))
  if toupper:
    lflank, rflank = lflank.upper(), rflank.upper()
    alleles = tuple(_.upper() for _ in alleles)
  strand = _identify_strand(lflank, alleles, rflank)
  if strand == 'BOT':
    lflank, alleles, rflank = (rc(_) for _ in (rflank, alleles, lflank))
  mask = (lflank, alleles, rflank)
  if rebuild_str:
    mask = join_mask(mask)
  return mask
