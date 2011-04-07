Example: basic computations on genotype data
============================================

This document describes how one can use the ``bl.vl.genotype```package
to perform basic computation on genotype data.

Motiviations
------------

The goal is to have a computationally efficient and reasonably clean
way to handle genotype data. By "genotype data", I mean:

  (a) the result of an high resolution device acquisition, e.g., what
  you get from an Affy 6.0 and an Illumina Infinium;

  (b) the result of interpolation and extrapolation of genotyping data
  to obtain even higher densities datasets.

A genotype data object GDO is composed by:

  (i)   a list of N markers ids that indentifies each snp in the datasets;

  (ii)  a type(np.zeros((2, N), dtype=np.float32)) array with the
        probabilities of the AA and BB configurations;
  (iii) a type(np.zeros((N,), dtype=np.float32)) array with the call confidence.

We are using a floating point representation, rather than the usual
'two-bits' per snp  representation because we are trying to mantain an
uniform description for both the 'experimental' and the 'computed' snp values.

All GDO coming from the same acquisition technology (both real and
synthetic) share the same marker list.

For the purposes of this discussion, what the markers are is
irrelevant. However, their details are currently handled as records of
a pytable table (omero.grid.table) with many millions of rows.

The typical operations that we expect to perform on the data can be
divided in, at least, three flavours.

 *columnar, per snp, statistics*: that is, use call values for the
  same marker across all the GDO in a given GDO set to compute a snp
  specific value, e.g., minimum allele frequency (MAF) and Hardy
  Weinberg equilibrium tests (HWE).

 *row, per GDO, statistics*: that is, use call values for all the snp
  within a GDO, for instance to count the number of
  (homo/hetero)zygotes snps.

 *group of GDO statistics*: that is, use a set to GDO to compute
  quantities that require to look at the same time in both the row and
  col directions, e.g., computing the Hamming distance (on snps)
  between pairs of GDOs, and using imputation techniques (e.g., with
  Merlin) to convert inter individual relations (pedigree)
  to deduce missing snp values.

Ideally, one would like to have server-side scripts that compute
'per-column' and 'per-row' quantities, and maybe, extract blocks of
GDOs and convert them to the format needed by external tools.  For
instance, saving in an HDFS directory a file with, for each snp, a record
containing the relative genotype for all the individuals in the
selected set.

Things like saving a new GDO will be left to the client, as well
as recovering a GDO.


