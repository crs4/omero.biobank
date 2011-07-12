How do we use Omero
===================

The GWAS version of OMERO uses extensively OmeroTable to handle
genotyping related information, with tables used to handle: snp
markers definitions; markers allignment to reference genomes; the
grouping of markers for different genotyping technologies; and the
genotyping call data proper stored as arrays of probability for
homozygotes calls.  By adopting this general storage approach, it
becomes easy to homogeneously handle data coming from different
genotyping technologies, e.g., from TaqMan(TM?) SNP genotyping assays
to calls obtained by the analysis of high throughput resequencing data
[ref?], and to process them very efficiently, since many of the basic
analysis, e.g., maf, Hardy Weinberg exact calculation
[Am.J.Hum.Genet.vol.76-pp.887, if we want to be pedantic] and other
data quality controls, can be expressed as parallel/streaming
operations across OmeroTable rows and be performed very close to I/O
bandwidth speed. [ref to opal bytesomething genome storing technology]

Omero core services are complemented by support python modules that
help in the packaging of data managed by Omero to application specific
formats. For instance, input data files (e.g., in the QTDT format [ref
QTDT?]) for pedigree analysis programs are build on the fly
parametrized by the specific individuals chosen, snp markers subset
and studied phenotypes.

As a general strategy, Omero is directly responsible of the managing
of all the available metadata, while specific large scale computations
are handled as a three steps processe: query omero for run
specification parameters; do the computation; if the computational
process was successful, store meta-information on the results in
Omero. By doing so, it is possible to maintain computational software
packages, e.g, map-reduce programs for GWAS [paper to be submitted]
and re-sequencing applications [2 2011 refs] usable by institutions
that do not yet use Omero.  It should be pointed out that for
massive... glue to below...

This problem is not only limited to analysis with large datasets, but
also appears in compute-intensive problems.  As an example, an
important component of the computational support for GWAS studies is
related to the interpolation/extrapolation of experimentally obtained
genotyping data to combine results across studies that relies on
different genotyping platforms and increase the power of individual
scans and imputing missing genotypes for a set of individuals by using
information available on their relatives. The duration of a single
imputation run depends strongly - from minutes to weeks of a standard
computing node -- on the complexity of the family tree structure. In a
large-scale population study it is usual to have batches of hundred of
thousands of runs for an imputation calculation and, as most compute
resources are shared resources, either within a department cluster or
in the Cloud, the efficient distribution and monitoring of compute
jobs becomes crucial. Without an accurate planning of computing
resources allocation, one could force large peak loads on the
computational infrastructure with no improvement of the total run
time: a naïve submission of a typical analysis run11 could completely
saturate a 400 8-core nodes cluster for hours with a swarm of quickly
completing jobs with only a few hundred jobs lasting for weeks. Figure
5B shows how the expected time needed for a typical imputation
computation depends on the number of computational nodes N used and
the maximal family "bit complexity" (BC, a measure of pedigree
complexity). The red line represents the set of optimal pairs of BC
and N. For a given BC, increasing the cluster size beyond the relative
optimal N value will not make the computation end sooner. Once the
overall computational parameters for the imputation job are set, the
batch of jobs can proceed, accessing the metadata and the specific
datasets contained in OMERO. This approach means that processing
metadata and setting up a calculation takes minutes at most, whereas
directly reading metadata from the raw data files (so-called, ped
files, 20 GB each) takes many hours.  This is important when these
calculations are run on shared computational resources or on fee-based
resources in the cloud.
