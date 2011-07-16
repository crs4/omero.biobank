import re
import itertools as it

T = {'A' : 'T', 'T' : 'A', 'G' : 'C', 'C': 'G'}
def conjugate(s):
  return ''.join(map(lambda _: T[_], s[::-1]))

def split_mask(mask):
  return re.split('[\[\/\]]', mask)

def identify_strand(mask):
  def is_unambiguos(p):
    l, r = p if p[0] < p[1] else (p[1], p[0])
    return not (l == r
                or (l == 'A' and r == 'T')
                or (l == 'C' and r == 'G'))
  #--
  mask = mask.upper()
  lflank, allele_a, allele_b, rflank = split_mask(mask)
  alleles = set([allele_a, allele_b])
  if 'A' in alleles and 'T' not in alleles:
    strand = 'TOP'
  elif 'T' in alleles and 'A' not in alleles:
    strand = 'BOT'
  else:
    # pesky case...
    for p in it.izip(lflank[::-1], rflank):
      if is_unambiguos(p):
        strand = 'TOP' if p[0] in 'AT' else 'BOT'
        break
    else:
      raise ValueError('Cannot decide strand of %s' % mask)
  return strand

def identify_strand_2(mask):
  def is_unambiguos(p):
    l, r = p
    l, r = (l, r) if l < r else (r, l)
    return not (l == r
                or (l == 'A' and r == 'T')
                or (l == 'C' and r == 'G'))
  #--
  mask = mask.upper()
  lflank, allele_a, allele_b, rflank = re.split('[\[\/\]]', mask)
  alleles = set([allele_a, allele_b])
  if 'A' in alleles and 'T' not in alleles:
    # we are in TOP
    alleles.remove('A')
    allele_a, allele_b = 'A', alleles.pop()
    lflank, rflank = lflank, rflank
    strand = 'TOP'
  elif 'T' in alleles and 'A' not in alleles:
    # we are in BOT, need to conjugate
    alleles.remove('T')
    allele_a, allele_b = 'A', conjugate(alleles.pop())
    lflank, rflank = conjugate(rflank), conjugate(lflank)
    strand = 'BOT'
  else:
    # pesky case...
    for p in it.izip(lflank, rflank[::-1]):
      if is_unambiguos(p):
        strand = 'TOP' if p[0] in 'AT' else 'BOT'
        break
    else:
      raise ValueError('Cannot decide strand of %s' % mask)
    if 'A' in alleles:
      assert 'T' in alleles
      allele_a, allele_b = 'A', 'T'
      if strand == 'TOP':
        lflank, rflank = lflank, rflank
      else:
        assert strand == 'BOT'
        lflank, rflank = conjugate(rflank), conjugate(lflank)
    else:
      assert 'C' in alleles  and 'G' in alleles
      allele_a, allele_b = 'C', 'G'
      if strand == 'TOP':
        lflank, rflank = lflank, rflank
      else:
        assert strand == 'BOT'
        lflank, rflank = conjugate(rflank), conjugate(lflank)
  return strand, allele_a, allele_b, lflank, rflank

def convert_to_top(mask):
  """
  Given a string mask with format <lflank>[AlleleA/AlleleB]<rflank>,
  it will convert to a new mask obtained by mapping mask to the TOP
  (following Illumina conventions) strand. See FIXME ref for algorithm
  description.
  """
  #--
  mask = mask.upper()
  lflank, allele_a, allele_b, rflank = split_mask(mask)
  #--
  allele_a, allele_b = ((allele_a, allele_b)
                        if allele_a < allele_b else (allele_b, allele_a))
  strand = identify_strand(mask)
  if strand == 'BOT':
    allele_a, allele_b = conjugate(allele_b), conjugate(allele_a)
    lflank, rflank = conjugate(rflank), conjugate(lflank)
  return lflank + ('[%s/%s]' % (allele_a, allele_b)) + rflank
