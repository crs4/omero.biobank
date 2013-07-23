Handling genotyping data
========================

One of OMERO.biobank's goals is to provide a clean and efficient way
to handle genotyping data, both "raw" (e.g., Affymetrix CEL files) and
"processed" (e.g., genotype calls (GCs), imputations).

GCs for an individual are stored as a Genotype Data Object (GDO),
which consists of:

* a 2 by N array with the probabilities of the AA and BB states;

* a 1 by N array with the corresponding confidence levels;

where N is the number of Single Nucleotide Polymorphism (SNP) markers
for the specific genotyping technology used for the assay. This
representation is used in lieu of the usual two-bits-per-SNP encoding
to provide a uniform description for both measured and interpolated
genotypes.  Marker information is currently stored in `OMERO tables
<https://www.openmicroscopy.org/site/support/omero4/developers/Tables.html>`_,
where each row corresponds to a marker.

Typical operations on genotyping data include:

* column-wise (per-SNP) statistics: use call values for the same SNP
  across all GDOs in a given set to compute SNP-specific values, e.g.,
  Minimum Allele Frequency (MAF) or Hardy-Weinberg Equilibrium (HWE).

* row-wise (per-GDO) statistics: use call values for all SNPs in a
  GDO, e.g., to count the number of heterozygous SNPs.

* GDO group statistics: use a set of GDOs to compute quantities that
  require looking at the same time in both the row and column
  directions, e.g., to compute the Hamming distance between pairs of
  GDOs or perform SNP imputation.
