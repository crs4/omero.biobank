import sys, os

import bl.vl.genotype.io as io
from bl.vl.kb import KnowledgeBase


OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASSWD = os.getenv("OME_PASSWD", "romeo")
MS_LABEL = os.getenv("MS_LABEL", "GDO_TEST_MS")
REF_GENOME = os.getenv("REF_GENOME", "hg19")


def get_data_samples(kb, mset):
  query = "from GenotypeDataSample g where g.snpMarkersSet.id = :id"
  params = {"id": mset.omero_id}
  return kb.find_all_by_query(query, params)


def main(argv):
  try:
    out_fn = argv[1]
  except IndexError:
    return "USAGE: python %s OUT_FN" % argv[0]
  kb = KnowledgeBase(driver="omero")(OME_HOST, OME_USER, OME_PASSWD)
  mset = kb.get_snp_markers_set(MS_LABEL)
  data_samples = get_data_samples(kb, mset)
  writer = io.VCFWriter(mset, REF_GENOME)
  with open(out_fn, "w") as fo:
    writer.write(fo, data_samples)
  print "wrote %s" % out_fn

if __name__ == "__main__":
  sys.exit(main(sys.argv))
