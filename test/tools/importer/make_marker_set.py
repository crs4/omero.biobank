# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, csv, random

USAGE = "Usage: python %s INPUT_F OUTPUT_F [SIZE]" % sys.argv[0]

try:
  in_fn = sys.argv[1]
  out_fn = sys.argv[2]
except IndexError:
  sys.exit(USAGE)
try:
  size = int(sys.argv[3])
except IndexError:
  size = None
except ValueError:
  sys.exit(USAGE+"\n\nSIZE must be an integer")


with open(in_fn) as fi, open(out_fn, 'w') as fo:
  i = csv.DictReader(fi, delimiter='\t')
  o = csv.DictWriter(fo, delimiter='\t', lineterminator="\n",
                     fieldnames=['marker_vid', 'marker_indx', 'allele_flip'])
  o.writeheader()
  records = [r for r in i]
  size = min(size, len(records))
  sample = records if size is None else random.sample(records, size)
  for k, r in enumerate(sample):
    y = {
      'marker_vid': r['vid'], 
      'allele_flip': random.choice([True, False]),
      'marker_indx': k,
      }
    o.writerow(y)
