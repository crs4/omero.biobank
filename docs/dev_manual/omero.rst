Biobank and OMERO
=================

Our modified version of OMERO makes extensive use of OMERO tables to
handle genotyping-related information:

* marker definitions;

* alignments of markers to reference genomes;

* marker sets for different genotyping technologies;

* genotype call data.

This approach allows to homogeneously handle data coming from
different technologies, from Affymetrix genotyping chips to SNP calls
based upon high-throughput deep sequencing. It also allows to
efficiently process such data to extract standard quality control
measures such as the Minor Allele Frequency (MAF).

OMERO core services are complemented by support Python modules that
export data as application-specific formats. For instance, input data
files for pedigree analysis tools are built on the fly, parametrized
by a specific set of chosen individuals, marker subset and relevant
phenotypes.

OMERO is directly responsible for the management of all available
metadata, while specific large-scale computation is performed as
three-step process:

#. query OMERO for job-specific parameters;

#. perform the computation proper;

#. if the above was successful, store meta-information on the results in OMERO.

This results in a high degree of decoupling between the OMERO backend
and the application domain, which allows to develop computational
software packages (e.g., MapReduce programs for GWAS [#chipal]_,
sequence alignment tools [#seal]_) that can be OMERO-agnostic --
at the expense of the metadata management features.

The problem of efficient metadata management is not limited to large
dataset analysis: it also affects computationally intensive
problems. For instance, an important component of the computational
support for GWAS studies is the interpolation or extrapolation of
experimental genotyping data to combine results across different
studies. This process -- imputing missing genotypes for a set of
individuals by using information on their relatives [#imputation]_ --
takes advantage of different genotyping platforms and increases the
overall statistical power with respect to single-technology scans.

The duration of a single imputation run depends strongly -- from
minutes to weeks on the average machine -- on the complexity of the
family tree structure. Thus, the efficient distribution and monitoring
of jobs becomes crucial. Processing metadata and setting up a
calculation with OMERO takes minutes at most, whereas directly reading
metadata from the raw data files may take many hours. This is
particularly important when such calculations are run on shared
computational resources or on fee-based resources in the cloud.


.. rubric:: Footnotes
	
.. [#chipal] S. Leo et al., *Efficient computing of genotype calling
   for GWAS*. Poster -- 12th International Congress of Human Genetics
   (ICHG) -- October 2011

.. [#seal] L. Pireddu et al., *SEAL: a distributed short read mapping
   and duplicate removal tool*. Bioinformatics 27(15):2159--2160, 2011.

.. [#imputation] Li et al., *Genotype Imputation*. Annual Review of
   Genomics and Human Genetics 10:387--406, 2009.
