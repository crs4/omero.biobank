import sys, time, csv, random
from bl.vl.utils.snp import convert_to_top

try:
  N = int(sys.argv[1])
except IndexError:
  N = 100
FLANK_SIZE = 20

with open('marker_definitions.tsv', 'w') as f:
  tsv = csv.DictWriter(f, fieldnames=['label', 'rs_label', 'mask'],
                       delimiter='\t', lineterminator="\n")
  tsv.writeheader()
  j = 0
  while j < N:
    lflank = ''.join([random.choice('ACGT') for i in xrange(FLANK_SIZE)])
    rflank = ''.join([random.choice('ACGT') for i in xrange(FLANK_SIZE)])
    alleles = '/'.join(random.sample('ACGT', 2))
    mask = '%s[%s]%s' % (lflank, alleles, rflank)
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
      'mask': mask
      }
    tsv.writerow(y)
