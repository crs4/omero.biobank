# BEGIN_COPYRIGHT
# END_COPYRIGHT

""" ..
FIXME:

.. todo::

  open a tsv with at least the 'allele_a', 'allele_b' and marker_vid columns
  fetch all markers corresponding to the VIDs and get their masks
  split mask and compare alleles
"""
import sys, os, csv
import itertools as it

from bl.vl.kb import KnowledgeBase as KB
from bl.core.seq.utils import reverse_complement as rc
import bl.vl.utils.snp as snp


OME_HOST   = os.getenv('OME_HOST', 'localhost')
OME_USER   = os.getenv('OME_USER', 'test')
OME_PASSWD = os.getenv('OME_PASSWD', 'test')

kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASSWD)

fn = sys.argv[1] # "affy_na32_reannot_vids.tsv"
outfn = sys.argv[2] # "affy_na32_markers_set_def.tsv"

with open(fn) as f:
  reader = csv.DictReader(f, delimiter="\t")
  records = [r for r in reader]

vids = [r['source'] for r in records]
markers = kb.get_snp_markers(vids=vids, col_names=['vid', 'mask'])

with open(outfn, 'w') as outf:
  fieldnames = ['marker_vid', 'marker_indx', 'allele_flip']
  writer = csv.DictWriter(outf, delimiter="\t", lineterminator=os.linesep,
                          fieldnames=fieldnames)
  writer.writeheader()
  for i, (m, r) in enumerate(it.izip(markers, records)):
    assert m.id == r['source']
    try:
      _, stored_alleles, _ = snp.split_mask(m.mask)
    except ValueError:
      sys.stdout.write("WARNING: could not split mask for %r\n" % r['source'])
      flip = False
    else:
      alleles = r['allele_a'], r['allele_b']
      fl_alleles = r['allele_b'], r['allele_a']
      if alleles == stored_alleles or rc(alleles) == stored_alleles:
        flip = False
      elif fl_alleles == stored_alleles or rc(fl_alleles) == stored_alleles:
        flip = True
      else:
        raise ValueError("%s: got inconsistent mask from db: %r" %
                         (m.id, m.mask))
    index = r.get("marker_indx", i)
    writer.writerow({"marker_vid": m.id, "marker_indx": index,
                     "allele_flip": flip})
