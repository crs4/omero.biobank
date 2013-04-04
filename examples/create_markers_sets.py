# BEGIN_COPYRIGHT
# END_COPYRIGHT

""" ..

The goal of this example is to show how marker sets are defined.

We will assume that, as explained in create_individuals.py, the kb
already contains a set of individuals enrolled in a study.

For each individual, we have a sample of DNA in a well of a work
plate, and we assume that we have run a series of TaqMan genotyping
assays that associates to each well a collection of genotyping
values. This is, of course, a simplistic model but it should be
enough to illustrate the procedure.

**Note:** DO NOT run this example on a production
  database. Also, what will be shown here is supposed to be
  didactically clear, not efficient. See the implementation of the
  importer tools for more efficient solutions.

First, as usual, we open the connection to the KnowledgeBase

"""

import os, uuid
import numpy as np
from bl.vl.kb import KnowledgeBase as KB

OME_HOST   = os.getenv('OME_HOST', 'localhost')
OME_USER   = os.getenv('OME_USER', 'test')
OME_PASSWD = os.getenv('OME_PASSWD', 'test')

STUDY_LABEL = os.getenv('STUDY_LABEL', 'TEST01')

kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASSWD)

""" ..

The following is the collection of (fake) marker data, where tuple
elements represent the label, mask, index and allele flip information
for each marker (see the marker set importer for more info).

"""

taq_man_markers = [
  ('A001', 'TCACTTCTTCAAAGCT[A/G]AGCTACAAGCATTATT', 0, False),
  ('A002', 'GGAAGGAAGAAATAAA[C/G]CAGCACTATGTCTGGC', 1, False),
  ('A003', 'CCGACCTAGTAGGCAA[A/G]TAGACACTGAGGCTGA', 2, False),
  ('A004', 'AGGTCTATGTTAATAC[A/G]GAATCAGTTTCTCACC', 3, True),
  ('A005', 'AGATTACCATGCAGGA[A/T]CTGTTCTGAGATTAGC', 4, False),
  ('A006', 'TCTACCTCTGTGACTA[C/G]AAGTGTTCTTTTATTT', 5, True),
  ('A007', 'AAGGCAATACTGTTCA[C/T]ATTGTATGGAAAGAAG', 6, True),
  ]

""" ..

Now we will load the above definitions into the kb:

"""

study = kb.get_study(STUDY_LABEL)

action = kb.create_an_action(study, doc='importing markers')
action.reload()

label, maker, model, release = (uuid.uuid4().hex for _ in xrange(4))
N, stream = len(taq_man_markers), iter(taq_man_markers)

mset = kb.create_snp_markers_set(
  label, maker, model, release, N, stream, action
  )

""" ..

We can assume that the markers above have been aligned against a
reference genome, and save the alignment information. The latter
consists of a stream of tuples where each tuple is in the marker vid,
chromosome number, position within the chromosome, strand info, number
of copies (see the docs for ``kb.align_snp_markers_set``):

"""

mset.load_markers()
aligns = [(m['vid'], i+1, (i+1)*1000, True, 'A' if (i%2)== 0 else 'B', 1)
          for i, m in enumerate(mset.markers)]
ref_genome = 'fake19'
kb.align_snp_markers_set(mset, ref_genome, iter(aligns), action)

""" ...

The function below will provide us with the data needed to fake the
results of a genotyping assay on the given SNPMarkersSet.  This is
modeled by a 2 X N (N = number of markers) array where each column
represents the probabilities of being homozygous on allele A and B,
and a 1 X N array where each element represents a measure of
confidence related to the measure contained at the same position in
the probabilities array.

"""

def make_fake_data(mset):
  n = len(mset)
  probs = 0.5 * np.cast[np.float32](np.random.random((2, n)))
  confs = np.cast[np.float32](np.random.random(n))
  return probs, confs

""" ..

Now that we have the SNPMarkersSet, we can proceed with the definition
of the datasets. We will consider a minimalistic model, where we do
not keep track of the Tube, PlateWell, ... chain that goes from the
individual to the experiment, but we directly link the dataset to the
individuals.

"""

data_sample_list = []
for i, ind in enumerate(kb.get_individuals(study)):
  action = kb.create_an_action(study, target=ind, doc='fake dataset')
  conf = {'label' : uuid.uuid4().hex,
          'status' : kb.DataSampleStatus.USABLE,
          'action' : action,
          'snpMarkersSet' : mset}
  data_sample = kb.factory.create(kb.GenotypeDataSample, conf).save()
  probs, conf = make_fake_data(mset)
  do = kb.add_gdo_data_object(action, data_sample, probs, conf)
  data_sample_list.append(data_sample)

""" ..

Note how we first create a ''data sample'' (GenotypeDataSample), which
keeps track of the existence of genotyping data defined on a given
marker set, and then we provide a ''data object'' that models the
physical object that actually contains the data. Multiple data objects
can link to the same data sample (they would contain the same data,
but on different file systems, or encoded in different formats).
"""

np.set_printoptions(precision=3)
print "marker set %s" % mset.id
for gdo in kb.get_gdo_iterator(mset, data_samples=data_sample_list):
  print gdo['probs']
  print gdo['confidence']
  print
