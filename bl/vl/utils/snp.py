import re
import itertools as it

from bl.core.seq.utils import reverse_complement as rc


MASK_PATTERN = re.compile(r'^([A-Z]+)\[([^/]+)/([^\]]+)\]([A-Z]+)$',
                          re.IGNORECASE)


def split_mask(mask):
  m = MASK_PATTERN.match(mask)
  try:
    lflank, allele_a, allele_b, rflank = m.groups()
  except AttributeError:
    raise ValueError("bad mask format: %r" % mask)
  else:
    return (lflank, (allele_a, allele_b), rflank)


def _identify_strand(lflank, alleles, rflank):
  def is_unambiguos(p):
    l, r = sorted(p)
    return not (l == r
                or (l == 'A' and r == 'T')
                or (l == 'C' and r == 'G'))
  #--
  alleles = set(alleles)
  if 'A' in alleles and 'T' not in alleles:
    strand = 'TOP'
  elif 'T' in alleles and 'A' not in alleles:
    strand = 'BOT'
  else:
    # pesky case...
    for p in it.izip(reversed(lflank), rflank):
      if is_unambiguos(p):
        strand = 'TOP' if p[0] in 'AT' else 'BOT'
        break
    else:
      raise ValueError('Cannot decide strand of %r' % mask)
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
  if rebuild_str:
    return '%s[%s/%s]%s' % (lflank, alleles[0], alleles[1], rflank)
  else:
    return (lflank, alleles, rflank)
