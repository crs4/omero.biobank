"""
Check strand designation against illumina annotation data.

Input file must be tab-separated with at least a 'mask' column, e.g.:

  mask
  CTGCAG[A/G]TGCTTGG
  GGCCCA[A/C]GCTGGGG
  ...

The mask column must contain illumina TOP masks, as found in the
'TopGenomicSeq' column of original annotation files. This program
prints the number of strands designed as BOT for non-indel SNPs, which
should be 0.
"""
import sys, os, csv
from bl.vl.utils.snp import split_mask, _identify_strand


BASES = set('ACGT')


class Reader(csv.DictReader):
  
  def __init__(self, f):
    csv.DictReader.__init__(self, f, delimiter="\t", quoting=csv.QUOTE_NONE)

  def next(self):
    r = csv.DictReader.next(self)
    try:
      mask = split_mask(r['mask'].upper())
    except ValueError, e:
      print "ERROR: %r: %s, skipping" % (label, e)
      return self.next()
    return mask


def main(argv):

  try:
    ann_fn = argv[1]
  except IndexError:
    p = os.path.basename(argv[0])
    print "Usage: python %s ANNOTATION_FILE" % p
    print __doc__
    sys.exit(2)

  bot_count = indel_count = 0
  with open(ann_fn) as f:
    n_records = sum(1 for _ in f) - 1
    feedback_step = n_records / 10
    f.seek(0)
    reader = Reader(f)
    for i, mask in enumerate(reader):
      lflank, alleles, rflank = mask
      if not set(alleles).issubset(BASES):
        indel_count += 1
        continue
      id_strand = _identify_strand(lflank, alleles, rflank)
      if id_strand != 'TOP':
        bot_count += 1
      if i % feedback_step == 0:
        print "%6.2f %% complete" % (100.*i/n_records)
  print 'indel count:', indel_count
  print 'BOT count:', bot_count


if __name__ == "__main__":
  main(sys.argv)
