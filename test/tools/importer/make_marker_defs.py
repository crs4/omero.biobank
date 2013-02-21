# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, time, csv, random
from bl.vl.utils.snp import convert_to_top

try:
  N = int(sys.argv[1])
except IndexError:
  N = 100
try:
  out_fn = sys.argv[2]
except IndexError:
  out_fn = 'marker_definitions.tsv'
FLANK_SIZE = 20
HEADER = ['label', 'rs_label', 'mask', 'allele_a', 'allele_b']


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
      convert_to_top(mask)
    except ValueError as e:
      print 'Bad mask, skipping'
      continue
    j += 1
    t = time.time() % 1000000
    y = {
      'label': 'foo-%d-%d' % (t, j),
      'rs_label': 'rs-foo-%d-%d' % (t, j),
      'mask': mask,
      'allele_a': alleles[0],
      'allele_b': alleles[1],
      }
    tsv.writerow(y)
