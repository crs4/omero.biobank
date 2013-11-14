# BEGIN_COPYRIGHT
# END_COPYRIGHT

# pylint: disable=W0105, C0103

""" ..

Loading a new Markers Array
===========================

In this example, we will, first, create a new markers array from an
Illumina annotation file and then create a new VariantCallSupport that
corresponds to the markers alignments against GRCh37.1 provided by
Illumina in the same file.
"""

""" ..

Creation of the new markers array
---------------------------------

We read the annotation file using the columns 

"""
from bl.core.io.illumina import IllSNPReader

def marker_stream(fname):
    for i, r in enumerate(IllSNPReader(open(fname))):
        yield {'label': r['IlmnID'], 'mask': r['TopGenomicSeq'], 
               'index': i, 'permutation' : False}

study = kb.get_study(STUDY_LABEL)
action = kb.create_an_action(study)
fname = 'human_exom-V1.csv'
stream = data_stream(fname)
label, maker, model, version = 'human_exom-V1', 'illumina', 'human_exom', 'V1'
mset = kb.genomics.create_markers_array(label, maker, model, version.
                                        stream, action)

""" ..

Creation of VariantCallSupport
------------------------------

FIXME

How to do this in general will be described in later example. Here we
will simply read the alignment information from the annotation file.
Note that, to simplify matters, we assume that we will have enough RAM
to be able to read everything in a single swoop.  

BTW, we know that it has been aligned against GRCh37.1 because it is
written in the annotation file.

"""

import numpy as np

n_markers = kb.genomics.get_number_of_markers(mset)
nodes = np.zeros(n_markers, kb.VariantCallSupport.NODES_DTYPE)
origin    = np.zeros(n_markers, kb.VariantCallSupport.ATTR_ORIGIN_DTYPE)

m_vid = mset.id
for i, r in enumerate(IllSNPReader(open(fname))):
    nodes[i] = (r['Chr'], r['MapInfo'])
    origin[i] = (i, m_vid, i)
    
reference_genome = kb.get_by_label(kb.ReferenceGenome, 'GRCh37.1')
conf = {'referenceGenome' : reference_genome,
        'label' : label,
        'status' : kb.DataSampleStatus.USABLE,
        'action': action}  
vcs = kb.factory.create(kb.VariantCallSupport, conf)
vcs.define_support(nodes)
vcs.define_field('origin', origin)
register_vcs(kb, vcs, action)







