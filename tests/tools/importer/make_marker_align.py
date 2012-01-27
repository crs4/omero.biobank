import sys, csv, random

FIELDS = ['marker_vid', 'chromosome', 'pos', 'allele', 'strand', 'copies']

try:
  in_fn = sys.argv[1]
  out_fn = sys.argv[2]
except IndexError:
  sys.exit("Usage: python %s INPUT_F OUTPUT_F" % sys.argv[0])

with open(in_fn) as fi, open(out_fn, 'w') as fo:
  i = csv.DictReader(fi, delimiter='\t')
  o = csv.DictWriter(fo, delimiter='\t', lineterminator="\n",
                     fieldnames=FIELDS)
  o.writeheader()
  for r in i:
    y = {
      'marker_vid': r['vid'],
      'chromosome': random.randrange(1, 26),
      'pos': random.randrange(1, 200000000),
      'allele': random.choice('AB'),
      'strand': random.choice([True, False]),
      'copies': 1
      }
    o.writerow(y)
