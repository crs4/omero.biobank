# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, csv, random

from bl.core.seq.utils import reverse_complement as rc
import bl.vl.utils.snp as snp_utils

try:
  N = int(sys.argv[1])
except IndexError:
  N = 100
try:
  out_fn = sys.argv[2]
except IndexError:
  out_fn = 'marker_definitions.tsv'
FLANK_SIZE = 20
HEADER = ['label', 'mask', 'index', 'allele_flip']


with open(out_fn, 'w') as f:
  tsv = csv.DictWriter(f, fieldnames=HEADER,
                       delimiter='\t', lineterminator="\n")
  tsv.writeheader()
  j = 0
  while j < N:
    lflank = ''.join([random.choice('ACGT') for i in xrange(FLANK_SIZE)])
    rflank = ''.join([random.choice('ACGT') for i in xrange(FLANK_SIZE)])
    alleles = random.sample('ACGT', 2)
    mask = '%s[%s]%s' % (lflank, '/'.join(alleles), rflank)
    try:
      mask = snp_utils.convert_to_top(mask)
    except ValueError as e:
      pass
    new_alleles = snp_utils.split_mask(mask)[1]
    if set(new_alleles) != set(alleles):
      new_alleles = rc(new_alleles)
      assert set(new_alleles) == set(alleles)
    y = {
      'label': 'M_%d' % j,
      'mask': mask,
      'index': j,
      'allele_flip': new_alleles[0] != alleles[0],
      }
    tsv.writerow(y)
    j += 1
