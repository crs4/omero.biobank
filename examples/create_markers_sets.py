""" ..

The goal of this example is to show how Markers and SNPMarkersSet are
defined.

We will assume that, as explained in FIXME, we already have a set of
individuals in omero and that they are enrolled in 'TEST01'.

For each individual, we have a sample of DNA in a well of a work
plate, and we assume that we have run a series of TaqMan genotyping
assays that associates to each well a collection of genotyping
values. This is, of course, a simplistic model but it should be
enough to illustrate the procedure.

**Note:** DO NOT run this examples against a production
  database. Also, what will be shown here is supposed to be
  didactically clear, not efficient. See the implementation of the
  importer tools for more efficiency solutions.


First, as usual, we open the connection to the KnowledgeBase
"""

from bl.vl.kb import KnowledgeBase as KB
import numpy as np

kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASSWD)


""" ..
The following is the collection of (fake) markers used for the TaqMan
assays.
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

Now we will load the markers set definition into Omero/VL.

**Note:** We are considering an ideal case where none of the markers
  are already in the db.

"""
study = kb.get_study('TEST01')

action = kb.create_an_action(study, doc='importing markers')

source, context, release = 'foobar', 'fooctx', 'foorel'

lvs = kb.create_markers(source, context, release, taq_man_markers, action)

taq_man_set = [ (t[1], i, False) for i, t in enumerate(lvs)]
label, maker, model, release = 'FakeTaqSet01', 'CRS4', 'TaqManSet', '23/09/2011'

mset = kb.create_snp_markers_set(label, maker, model, release,
                                 taq_man_set, action)

""" ...

The function below will provide us with the data needed to fake the
results of a genotyping assays on the given SNPMarkersSet.  As
discussed in XXXX, we need a numpy array of shape '''(2, N)''', where
'''N''' is the number of markers, that represents the probability of
being homozygous on allele A and allele B, and a numpy array of shape
'''(N,)''' that represent the relative confidence on the measurements.

.. todo::

   write a general discussion on the floating point approach to
   genotyping data storage.

"""

def make_fake_data(mset):
  n = len(mset)
  probs = 0.5 * np.cast[np.float32](np.random.random((2, n)))
  confs = np.cast[np.float32](np.random.random(n))
  return probs, confs

""" ..

Now that we have the SNPMarkersSet, we can proceed with the definition
of the datasets. We will consider a minimalistic model, where we will
not keep track of the Tube, PlateWell, ... chain that goes from the
individual to the experiment, but we directly link the dataset to the
individuals. To register this dependency and how the dependency was
established, each DataSample object is linked with an action to the
the relevant Individual instance. Mainly for historical reasons, the
action is though as an arrow that starts from the 'newer' object, the
GenotypeDataSample instance in this case, and has as a 'target' the
object on which the process operated. It is, essentially, a
representation of the 'inverse' of the creation process.

.. todo::

   uhmmm, not sure that the Action explanation above is crystal clear :-).

"""

data_sample_by_id = {}
family = []
for i, ind in enumerate(kb.get_individuals(study)):
  family.append(ind)
  action = kb.create_an_action(study, target=ind, doc='fake dataset')
  conf = {'label' : 'taq-%03d' % i,
          'status' : kb.DataSampleStatus.USABLE,
          'action' : action,
          'snpMarkersSet' : mset}
  data_sample = kb.factory.create(kb.GenotypeDataSample, conf).save()
  probs, conf = make_fake_data(mset)
  do = kb.add_gdo_data_object(action, data_sample, probs, conf)
  data_sample_by_id[ind.id] = data_sample

""" ..

As an example, we will now write out the information we have just
saved as a plink pedfile.

"""
from bl.vl.genotype.io import PedWriter

pw = PedWriter(mset, base_path="./foo")

pw.write_map()

family_label = study.label
pw.write_family(family_label, family, data_sample_by_id)

pw.close()





