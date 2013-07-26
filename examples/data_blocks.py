# BEGIN_COPYRIGHT
# END_COPYRIGHT

# pylint: disable=W0105, C0103

""" ..

This example shows how to manipulate genomic data stored in the KB and
perform basic QC measures.

**NOTE:** the example assumes that the KB already contains all objects
created by the examples on importing individuals and marker sets.

"""

import os
from bl.vl.kb import KnowledgeBase as KB
import bl.vl.genotype.algo as algo

OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'test')
OME_PASSWD = os.getenv('OME_PASSWD', 'test')
STUDY_LABEL = 'KB_EXAMPLES'
MSET_LABEL = 'DUMMY_MS'
REF_GENOME = 'DUMMY_GENOME'

kb = KB(driver="omero")(OME_HOST, OME_USER, OME_PASSWD)
mset = kb.get_snp_markers_set(label=MSET_LABEL)
mset.load_markers()

""" ..

The following snippet shows how to use part of a marker set's interface:

"""

print "%s: %s (%d markers)" % (mset.__class__.__name__, mset.label, len(mset))
for k, m in enumerate(mset):
    print "%s #%d: label=%s, mask=%s. pos=%r" % (
        m.__class__.__name__, k, m.label, m.mask, m.position
        )

""" ..

Now we will build a list of all data samples that refer to ``mset``
and are linked to an individual enrolled in a specific study.  To keep
things simple, for each individual we will select the first known data
sample that refers to ``mset``; if no such data sample exist, we will
skip the individual.

"""

def extract_data_samples(study, marker_set, class_name):
    by_individual = {}
    for i in kb.get_individuals(study):
        gds_i = [ds for ds in kb.get_data_samples(i, class_name)
                 if ds.snpMarkersSet == marker_set]
        if len(gds_i) < 1:
            continue
        by_individual[i.id] = gds_i[0]
    return by_individual

test_study = kb.get_study(label=STUDY_LABEL)
gds_by_individual = extract_data_samples(test_study, mset, 'GenotypeDataSample')

""" ..

Now we can perform QC on data objects available for the selected data samples:

"""

def do_check(s):
    counts = algo.count_homozygotes(s)
    return algo.maf(None, counts), algo.hwe(None, counts)

gds = gds_by_individual.values()
stream = kb.get_gdo_iterator(mset, data_samples=gds)
mafs, hwe = do_check(stream)

""" ..

Same as above, but on an arbitrary subset of the marker set:

"""

stream = kb.get_gdo_iterator(
    mset, data_samples=gds, indices=slice(1, len(mset)-1)
    )
mafs, hwe = do_check(stream)

""" ..

Same as above, but on a subset of the marker set based on the genomic
coordinates of the markers.

"""

mset.load_alignments(REF_GENOME)
chrom_start = chrom_end = 10
start, end = 63000000, 116000000
indices = kb.SNPMarkersSet.define_range_selector(
    mset, gc_range=((chrom_start, start), (chrom_end, end))
    )
stream = kb.get_gdo_iterator(
    mset, indices=indices, data_samples=gds
    )
mafs, hwe = do_check(stream)
