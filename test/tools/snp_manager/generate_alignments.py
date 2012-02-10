import sys, csv, random
from bl.vl.app.snp_manager.common import MARKER_AL_FIELDS


FRACTION = .9  # generate FRACTION * N_OF_MARKER_DEFS alignments


def get_labels(mdef_fn, fraction):
  with open(mdef_fn) as f:
    reader = csv.DictReader(f, delimiter="\t")
    labels = [r['label'] for r in reader]
    return labels[:int(fraction*len(labels))]


def main(argv):
  try:
    mdef_fn = argv[1]
    out_fn = argv[2]
    ref_gen = argv[3]
  except IndexError:
    sys.exit("Usage: %s MDEF_FN OUT_FN REF_GENOME" % argv[0])
  labels = get_labels(mdef_fn, FRACTION)
  with open(out_fn, "w") as fo:
    writer = csv.DictWriter(
      fo, MARKER_AL_FIELDS, delimiter="\t", lineterminator="\n"
      )
    writer.writeheader()
    _choice = random.choice
    _randint = random.randint
    for l in labels:
      copies = _randint(1, 2)
      for i in xrange(copies):
        r = dict(
          marker_vid=l,
          ref_genome=ref_gen,
          chromosome=str(_randint(1,26)),
          pos=str(_randint(1, 200000000)),
          strand=_choice('-+'),
          allele=_choice('AB'),
          copies=str(copies),
          )
        writer.writerow(r)


if __name__ == "__main__":
  main(sys.argv)
