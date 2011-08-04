"""
Convert SNP data from Affymetrix csv chip annotation files.
"""

import sys, os, csv
from contextlib import nested


FIELDS = "Probe Set ID", "dbSNP RS ID", "Flank", "Allele A", "Allele B"
OUT_FIELDS = "label", "rs_label", "mask", "allele_a", "allele_b"


def comment_filter(f):
  for line in f:
    if line[0] != '#':
      yield line


def main(argv):
  try:
    fn = argv[1]
    out_fn = argv[2]
  except IndexError:
    p = os.path.basename(argv[0])
    print "Usage: python %s ANNOTATION_FILE OUTPUT_FILE" % p
    print __doc__
    print "Example: python %s GenomeWideSNP_6.na31.annot.csv out.tsv" % p
    sys.exit(2)
  with nested(open(fn), open(out_fn, 'w')) as (f, outf):
    n_records = sum(1 for _ in f)
    feedback_step = n_records / 10
    f.seek(0)
    reader = csv.DictReader(comment_filter(f))
    writer = csv.writer(outf, delimiter="\t", quoting=csv.QUOTE_NONE,
                        lineterminator="\n")
    writer.writerow(OUT_FIELDS)
    for i, r in enumerate(reader):
      writer.writerow([r[k] for k in FIELDS])
      if i % feedback_step == 0:
        print "%6.2f %% complete" % (100.*i/n_records)


if __name__ == "__main__":
  main(sys.argv)
