import re
import itertools as it

from bl.core.seq.utils import reverse_complement as rc


MASK_PATTERN = re.compile(r'^([A-Z]+)\[([^\]]+)\]([A-Z]+)$', re.IGNORECASE)


UNAMBIGUOUS = {
  frozenset(['A', 'C']): 'TOP',
  frozenset(['A', 'G']): 'TOP',
  frozenset(['C', 'T']): 'BOT',
  frozenset(['G', 'T']): 'BOT',
  frozenset(['A', 'C', 'G']): 'TOP',
  frozenset(['A', 'C', 'T']): 'TOP',
  frozenset(['A', 'G', 'T']): 'BOT',
  frozenset(['C', 'G', 'T']): 'BOT',
  }


SNP_FLANK_SIZE = 125
SNP_MASK_SIZE = 2*SNP_FLANK_SIZE+1


def split_mask(mask):
  m = MASK_PATTERN.match(mask)
  try:
    lflank, alleles, rflank = m.groups()
  except AttributeError:
    raise ValueError("bad mask format: %r" % mask)
  else:
    return (lflank, tuple(alleles.split("/")), rflank)


def join_mask(mask):
  try:
    return '%s[%s]%s' % (mask[0], "/".join(mask[1]), mask[2])
  except IndexError:
    raise ValueError("bad mask format: %r" % (mask,))


def rc_mask(mask):
  return tuple(map(rc, reversed(mask)))


def _identify_strand(lflank, alleles, rflank):
  """
  Perform strand designation according to the Illumina convention.

  NOTE: expects all parameters to be uppercase.
  """
  try:
    return UNAMBIGUOUS[frozenset(alleles)]
  except KeyError:
    for p in it.izip(reversed(lflank), rflank):
      if frozenset(p) in UNAMBIGUOUS:
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
  mask = (lflank, alleles, rflank)
  if strand == 'BOT':
    mask = rc_mask(mask)
  if rebuild_str:
    mask = join_mask(mask)
  return mask
