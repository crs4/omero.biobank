""" ..

The goal of this example is to show how Markers and SNPMarkersSet are
defined.

**Note:** DO NOT run this examples against a production database.

We will consider a set of TaqMan experiments run against the following
set of SNP (fake) markers.
"""
taq_man_markers = [
  ('A0001', 'xrs122652',  'TCACTTCTTCAAAGCT[A/G]AGCTACAAGCATTATT'),
  ('A0002', 'xrs741592',  'GGAAGGAAGAAATAAA[C/G]CAGCACTATGTCTGGC'),
  ('A0003', 'xrs807079',  'CCGACCTAGTAGGCAA[A/G]TAGACACTGAGGCTGA'),
  ('A0004', 'xrs567736',  'AGGTCTATGTTAATAC[A/G]GAATCAGTTTCTCACC'),
  ('A0005', 'xrs4693427', 'AGATTACCATGCAGGA[A/T]CTGTTCTGAGATTAGC'),
  ('A0006', 'xrs4757019', 'TCTACCTCTGTGACTA[C/G]AAGTGTTCTTTTATTT'),
  ('A0007', 'xrs7958813', 'AAGGCAATACTGTTCA[C/T]ATTGTATGGAAAGAAG')
  ]

""" ..

The first element of a marker defining tuple is its label, the second
is the dbSNP db label, if available, while the third is the marker mask.

.. todo::

  put a reference to reference documentation

The first thing we will do is now to load the markers set definition
into Omero/VL.

**Note:** We are considering an ideal case where none of the markers
  are already in the db.

"""

from bl.vl.kb import KnowledgeBase as KB
from examples_common import create_an_action, create_a_study

kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASSWD)

study = kb.get_study('TEST01')
if not study:
  study = create_a_study('TEST01')

action = create_an_action(study, 'importing markers')

source, context, release = 'foobar', 'fooctx', 'foorel'

lvs = kb.create_markers(kb, source, context, release, taq_man_markers, action)

taq_man_set = [ (t[1], i, False) for i, t in enumerate(lvs)]
label, maker, model, release = 'FakeTaqSet01', 'CRS4', 'TaqManSet', '23/09/2011'

mset = kb.create_snp_markers_set(label, marker, model, release,
                                 taq_man_set, action)

""" ..
Now that we have the SNPMarkersSet, we can add some (fake) datasets.

"""






