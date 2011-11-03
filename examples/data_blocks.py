""" ..

Genomic data set manipulations
------------------------------

.. todo::

  We are missing some sort of introduction.

FIXME: Here goes a general intro. Points to touch:
 * A major complication is to decide what stays in memory and what
   should be projected in db.
 * For the time being, we try to be clear first and efficient second.
 * Explanations concerning how to open a connection to a KnowledgeBase
   and what it is are left to other, antecedent, parts of the
   documentation.

"""

from bl.vl.kb import KnowledgeBase as KB
import itertools as it

kb = KB(driver="omero")(OME_HOST, OME_USER, OME_PASSWD)

""" ..

The first thing we will do is to select a markers set. See FIXME:XXX
for its definition. We will first obtain an handle to it, and then
invoke a '.load_markers()' that will bring in memory the actual definition
data.
"""

mset_name = 'FakeTaqSet01'
mset0 = kb.get_snp_markers_set(label=mset_name)
mset0.load_markers()

""" ..

For the time being, we can think the SNPMarkerSet mset0 as analogous to an array
of markers. The following is a list of expressions that are expected
to be legal.
"""
len(mset0)
mset0[0::10]
mset0[11]
mset0[1].label
mset0[1].rs_label

""" ..

Note that, apart from the len(mset0), all this information will not
be available unless one requests a '.load_markers()', see above. The latter,
however, is a rather expensive operation that could take a
considerable time and require large amounts of memory. E.g., a 1M
markers set, will result in an object of about (48+32+133) * 1M bytes.

Now we will build a list of all the GenotypeDataSample objects
supported on mset0, linked to an individual contained in a given
group. Just to keep things simple, we will select, for each
individual, the first of the list of known GenotypeDataSample for that
mset, if there is at least one, otherwise we will skip the individual.
"""
def extract_data_sample(group, mset, dsample_name):
  by_individual = {}
  for i in kb.get_individuals(group):
    gds = filter(lambda x: x.snpMarkersSet == mset,
                 kb.get_data_samples(i, dsample_name))
    assert(len(gds) == 1)
    by_individual[i.id] = gds[0]
  return by_individual

group = kb.get_study(label='TEST01')
gds0_by_individual = extract_data_sample(group, mset0, 'GenotypeDataSample')

""" ..

Note that what we have now is a dictionary that maps individual ids to
GenotypeDataSample objects  and the latter are only handlers to get to
the actual genotyping data, not the data itself.

We can, now, do a global check on data quality.
"""
def do_check(s):
  counts = algo.count_homozygotes(s)
  mafs = algo.maf(None, counts)
  hwe  = algo.hwe(None, counts)
  return mafs, hwe

gds_0_data_samples = gds0_by_individual.values()
s = kb.get_gdo_iterator(mset0, data_samples=gds_0_data_samples)
mafs, hwe = do_check(s)

""" ..
Similar to above, but now we subselect on a slice of the markers
"""

s = kb.get_gdo_iterator(mset0, indices=slice(0, len(mset0), 100),
                        data_samples=gds_0_data_samples)
mafs, hwe = do_check(s)

""" ..
Same as above, but now we work on a subset of possible markers
selected as the one that have genomic coordinates in the interval
defined as from gc_begin (included) to gc_end (excluded).
"""

ref_genome = 'hg19'
begin_chrom = 10
begin_pos = 190000
end_chrom = 10
end_pos = 300000

gc_begin=(ref_genome, begin_chrom, begin_pos)
gc_end  =(ref_genome, end_chrom, end_pos)

indices = kb.SNPMarkersSet.extract_range(mset0, gc_range=(gc_begin, gc_end))

""" ..
this subranging will clearly fail if the markers in mset0 have not
been aligned against the reference genome.
"""

s = kb.get_gdo_iterator(
  mset0, indices=indices,
  data_samples=[x.id for x in gds_0_by_individual.itervalues()]
  )
mafs, hwe = do_check(s)

""" ..
Let's now suppose that we have obtained genotyping data with two
different technologies ('foo' and 'bar') and we would like to compare
results on the shared markers.
"""
mset1 = kb.get_snp_markers_set(label="bar")
gds_1_by_individual = extract_data_sample(group, mset1, 'GenotypeDataSample')

#--- ??? ---
data_sample_0 = []
data_sample_1 = []
for k in gds_0_by_individual:
  if k in gds_1_by_individual:
    data_sample_0.append(gds_0_by_individual[k])
    data_sample_1.append(gds_1_by_individual[k])
#-----------

indices_0, indices_1 = kb.SNPMarkersSet.intersect(mset0, mset1)

""" ..
To compare we use a suitable function such as the following:

.. todo::

   TBD

"""
def compare(a, b):
  pass

for (a, b) in it.izip(kb.get_gdo_iterator(mset0, indices=indices_0,
                                          data_samples=data_sample_0),
                      kb.get_gdo_iterator(mset1, indices=indices_1,
                                          data_samples=data_sample_1)):
  compare(a, b)
