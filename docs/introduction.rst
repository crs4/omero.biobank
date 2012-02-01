Introduction
============

Biobank is a framework built upon `OMERO
<http://www.openmicroscopy.org/site/products/omero>`_ that provides
compact and efficient ways to handle the computational aspects of
large scale biomedical studies.

Biobank's infrastucture, API and applications provide models for all
relevant objects, from laboratory samples to EHR archetypes, together
with data structures and patterns for describing and tracking the
actions that link one to the other. Biobank's main command line tools
have a `Galaxy <http://usegalaxy.org>`_ front-end.


Biobank's data model
--------------------

.. todo::

  write this section.


Basic computation on genotype data
----------------------------------

The goal is to provide a computationally efficient and reasonably clean
way to handle genotype data, i.e.:

* the results of high-resolution scanning with devices such as
  Affymetrix 6.0 and an Illumina Infinium;

* the results of interpolations and extrapolations of genotyping
  data to obtain even higher densities data sets.

A Genotype Data Object (GDO) consists of:

* a list of N Single Nucleotide Polymorphism (SNP) ids;

* a ``type(np.zeros((2, N), dtype=np.float32))`` array with the
  probabilities of the AA and BB configurations;

* a ``type(np.zeros((N,), dtype=np.float32))`` array with the call confidence.

The above floating point representation is used in lieu of the usual
two-bits-per-SNP representation to provide a uniform description for
both the 'experimental' and the 'computed' calls. All GDOs coming from
the same acquisition technology (either physical or synthetic) share
the same marker list.

For the purposes of this discussion, what the markers are is
irrelevant. However, their details are currently handled by records in
an OMERO table whose order of magnitude is in the millions of rows.

The typical operations that have to be performed on the data are:

* column-wise, per-SNP, statistics: use call values for the same SNP
  across all the GDOs in a given GDO set to compute a SNP-specific
  value, e.g., Minimum Allele Frequency (MAF) or Hardy-Weinberg
  Equilibrium (HWE).

* row-wise, per-GDO, statistics: use call values for all the SNPs
  within a GDO, e.g., to count the number of heterozygous SNPs.

* GDO group statistics: use a set of GDOs to compute quantities that
  require looking at the same time in both the row and column
  directions, e.g., to compute the Hamming distance (on SNPs) between
  pairs of GDOs; perform SNP imputation, e.g., with Merlin.

Ideally, server-side scripts should compute 'per-column' and 'per-row'
quantities, extract blocks of GDOs and convert them to the format
needed by external tools. For instance, saving in an HDFS directory a
file with, for each SNP, a record containing the genotype for all the
individuals in the selected set. Things like saving a new GDO are left
to the client.
