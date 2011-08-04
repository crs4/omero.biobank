"""
Convert SNP data from Illumina csv chip annotation files.
"""

import sys, os, csv, re
from contextlib import nested


OUT_FIELDS = "label", "rs_label", "mask", "strand", "allele_a", "allele_b"
RS_PATTERN = re.compile(r'rs\d+')


def ill_ann_filter(ann_f):
  open = False
  for line in ann_f:
    if line.startswith('[Assay]'):
      open = True
      continue
    elif line.startswith('[Controls]'):
      open = False
    if open:
      yield line


def main(argv):

  try:
    ann_fn = argv[1]
    out_fn = argv[2]
  except IndexError:
    p = os.path.basename(argv[0])
    print "Usage: python %s ANNOTATION_FILE OUTPUT_FILE" % p
    print __doc__
    print "Example: python %s Human1M-Duov3_B.csv VL_Human1M-Duov3_B.tsv" % p
    sys.exit(2)

  cnv_counter = 0
  with nested(open(ann_fn), open(out_fn, "w")) as (f, outf):
    n_records = sum(1 for _ in f)
    feedback_step = n_records / 10
    f.seek(0)
    reader = csv.DictReader(ill_ann_filter(f), quoting=csv.QUOTE_NONE)
    outf.write("%s\n" % "\t".join(OUT_FIELDS))
    for i, r in enumerate(reader):
      if r['CNV_Probe'] == '1':
        cnv_counter += 1
        continue
      a, b = r['SNP'][1:-1].split('/')
      rs_id = r['Name'] if RS_PATTERN.match(r['Name']) else 'None'
      outf.write("%s\n" % "\t".join(
        [r['IlmnID'], rs_id, r['TopGenomicSeq'], r['IlmnStrand'], a, b]
        ))
      if i % feedback_step == 0:
        print "%6.2f %% complete" % (100.*i/n_records)

  print "wrote %r" % out_fn
  print "CNV count:", cnv_counter


if __name__ == "__main__":
  main(sys.argv)
