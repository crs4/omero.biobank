# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, csv
from random import choice, uniform

from bl.core.io import MessageStreamWriter

PAYLOAD_TYPE = "core.gt.messages.SampleSnpCall"
MAP_FILE = "ssc_map.tsv"


def write_an_ssc(sample_id, snp_labels, out_dir):
  call_values = [2, 3, 4, 5]
  conf_range = [0.0, 0.9]
  sig_range = [1e2, 1e4]
  w_range = [0, 1e-5]
  hdr = {'sample_id': sample_id}
  fn = '%s.ssc' % sample_id
  path = os.path.join(out_dir, fn)
  print "writing", path
  with open(path, "w") as fo:
    writer = MessageStreamWriter(fo, PAYLOAD_TYPE, hdr)
    for snp in snp_labels:
      writer.write({
        'sample_id': sample_id,
        'snp_id': snp,
        'call': choice(call_values),
        'confidence': uniform(*conf_range),
        'sig_A': uniform(*sig_range),
        'sig_B': uniform(*sig_range),
        'w_AA': uniform(*w_range),
        'w_AB': uniform(*w_range),
        'w_BB': uniform(*w_range),
        })
  return fn


def get_labels(tsv_fn):
  with open(tsv_fn) as fi:
    reader = csv.DictReader(fi, delimiter='\t')
    return [r["label"] for r in reader]


def main():
  USAGE = "Usage: python %s MDEF_F IND_F [OUTPUT_D]" % sys.argv[0]
  try:
    marker_defs_fn = sys.argv[1]
    individuals_fn = sys.argv[2]
  except IndexError:
    sys.exit(USAGE)
  try:
    out_dir = sys.argv[3]
  except IndexError:
    out_dir = "ssc"
  #--
  if not os.path.isdir(out_dir):
    os.makedirs(out_dir)
  snp_labels = get_labels(marker_defs_fn)
  ind_labels = get_labels(individuals_fn)
  with open(MAP_FILE, "w") as fo:
    fo.write("ssc_label\tsource_label\n")
    for l in ind_labels:
      fn = write_an_ssc(l, snp_labels, out_dir)
      fo.write("%s\t%s\n" % (fn, l))
  print "wrote %s" % MAP_FILE


if __name__ == "__main__":
  main()
