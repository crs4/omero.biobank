# BEGIN_COPYRIGHT
# END_COPYRIGHT

# pylint: disable=W0105, C0103

""" ..

This example shows how to handle genetic marker data.

**NOTE:** the example assumes that the KB already contains all objects
created by the example on importing individuals.

Suppose you have run a series of genotyping assays where the DNA
sample in each well of a titer plate has been associated to a
collection of genotyping values.  To import this information into the
KB, we define a collection of marker data:

"""

import sys, os, uuid
import numpy as np
from bl.vl.kb import KnowledgeBase as KB

OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'test')
OME_PASSWD = os.getenv('OME_PASSWD', 'test')
STUDY_LABEL = 'KB_EXAMPLES'
MSET_LABEL = 'DUMMY_MS'
REF_GENOME = 'DUMMY_GENOME'

kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASSWD)
marker_defs = [
    # label   mask                                   index allele_flip
    ('A001', 'TCACTTCTTCAAAGCT[A/G]AGCTACAAGCATTATT',  0,  False),
    ('A002', 'GGAAGGAAGAAATAAA[C/G]CAGCACTATGTCTGGC',  1,  False),
    ('A003', 'CCGACCTAGTAGGCAA[A/G]TAGACACTGAGGCTGA',  2,  False),
    ('A004', 'AGGTCTATGTTAATAC[A/G]GAATCAGTTTCTCACC',  3,  True),
    ('A005', 'AGATTACCATGCAGGA[A/T]CTGTTCTGAGATTAGC',  4,  False),
    ('A006', 'TCTACCTCTGTGACTA[C/G]AAGTGTTCTTTTATTT',  5,  True),
    ('A007', 'AAGGCAATACTGTTCA[C/T]ATTGTATGGAAAGAAG',  6,  True),
    ]

""" ..

See the :ref:`import tool documentation <import_tool>` for
details on the mask, index and allele flip fields.  Now we have to
import the above definitions into the KB:

"""

study = kb.get_study(STUDY_LABEL)
if study is None:
    sys.exit("ERROR: study '%s' not found" % STUDY_LABEL)
action = kb.create_an_action(study)
maker, model, release = (uuid.uuid4().hex for _ in xrange(3))
N, stream = len(marker_defs), iter(marker_defs)
mset = kb.create_snp_markers_set(
    MSET_LABEL, maker, model, release, N, stream, action
    )

""" ..

If markers have been aligned to a reference genome, we can store the
alignment information.  This information must be provided in the form
of a stream of tuples that contain the marker's id within the KB
(called *vid*), chromosome number, position within the chromosome,
strand info and number of copies.  Again, the :ref:`import tool docs
<import_tool>` provide more details on this matter.  In this case, we
will auto-generate dummy alignment info for all markers in the set:

"""

mset.load_markers()
aligns = [(m['vid'], i+1, (i+1)*1000, True, 'A' if (i%2)== 0 else 'B', 1)
          for i, m in enumerate(mset.markers)]
kb.align_snp_markers_set(mset, REF_GENOME, iter(aligns), action)

""" ...

In OMERO.biobank, genotyping data is represented by a pair of arrays:

* a 2 X N array where each column represents the probabilities of
  being homozygous for allele A and B, respectively;

* a 1 X N array where each element represents a degree of confidence
  related to the corresponding probabilities in the above array;

where N is the number of markers in the reference set.  The following
snippet generates dummy genotyping data for all individuals enrolled
in the study:

"""

def make_dummy_data(ms):
    n = len(ms)
    probabilities = 0.5 * np.cast[np.float32](np.random.random((2, n)))
    confidence_values = np.cast[np.float32](np.random.random(n))
    return probabilities, confidence_values

data_sample_list = []
for i, ind in enumerate(kb.get_individuals(study)):
    action = kb.create_an_action(study, target=ind)
    config = {
        'label' : uuid.uuid4().hex,
        'status' : kb.DataSampleStatus.USABLE,
        'action' : action,
        'snpMarkersSet' : mset
        }
    data_sample = kb.factory.create(kb.GenotypeDataSample, config).save()
    probs, confs = make_dummy_data(mset)
    do = kb.add_gdo_data_object(action, data_sample, probs, confs)
    data_sample_list.append(data_sample)

""" ..

Data samples keep track of the existence of genotyping data defined on
a given marker set, while data objects model actual data containers
such as files or OMERO table rows.  Multiple data objects can refer to
the same data sample when they contain the same data, but encoded in
different formats, or stored in distinct copies of the same file.

For simplicity, we have defined an action that directly links each
data sample to an individual.  While this approach can be used when no
information is available on the steps that led to the production of
the data sample, the KB allows to keep track of several intermediate
objects such as blood samples, dna samples, titer plates, plate wells,
etc.  To iterate over the data objects we have just stored, we can do
the following:
"""

np.set_printoptions(precision=3)
print "marker set id: %s" % mset.id
for gdo in kb.get_gdo_iterator(mset, data_samples=data_sample_list):
    print gdo['probs']
    print gdo['confidence']
    print
