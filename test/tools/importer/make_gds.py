# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, csv
from contextlib import nested

USAGE = "Usage: python %s INPUT_F OUTPUT_F MSET DEVICE_VID" % sys.argv[0]

try:
  in_fn = sys.argv[1]
  out_fn = sys.argv[2]
  mset = sys.argv[3]
  device_vid = sys.argv[4]
except IndexError:
  sys.exit(USAGE)


with nested(open(in_fn), open(out_fn, 'w') as (fi, fo):
  i = csv.DictReader(fi, delimiter='\t')
  o = csv.DictWriter(fo, delimiter='\t', lineterminator="\n",
                     fieldnames=['label', 'source', 'device', 'status'])
  o.writeheader()
  for k, r in enumerate(i):
    y = {
      'label': r['group_code'] + '.' + mset,
      'source': r['individual'],
      'device': device_vid,
      'status': "USABLE",
      }
    o.writerow(y)
