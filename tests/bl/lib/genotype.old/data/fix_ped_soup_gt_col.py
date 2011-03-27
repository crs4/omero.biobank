import sys, os

BASE = "/home/simleo/svn/bl/vl/genotype/trunk/tests/bl/lib/genotype/data"
PED_SOUP = os.path.join(BASE, "ped_soup.ped")
BROKEN_PED = os.path.join(BASE, "broken.ped")

def get_data(fn, skip_first=False):
  data = []
  f = open(fn)
  n_skipped = 0
  for line in f:
    line = line.split()
    try:
      int(line[1])
    except (IndexError, ValueError):
      n_skipped += 1  # blank or invalid or individual label is not numeric
      continue
    data.append(line)
  f.close()
  print "%r: skipped %d records" % (fn, n_skipped)
  return data


def main(argv):
  ped_soup_data = get_data(PED_SOUP)
  broken_ped_data = get_data(BROKEN_PED, skip_first=True)
  gt_map = dict((r[1], r[5:]) for r in broken_ped_data)
  is_gt = lambda r: r[2] == r[3] == r[7] == "x"
  f = open("%s.fixed" % PED_SOUP, "w")
  for r in ped_soup_data:
    gt_info = gt_map[r[1]]
    r[-1] = str(int(is_gt(gt_info)))
    f.write("%s\n" % " ".join(r))
  f.close()


if __name__ == "__main__":
  main(sys.argv)
