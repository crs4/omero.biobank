
Example: missing genotype imputation
====================================


This document describes how it is possible to use this package to
support genomic imputation. 


From SNP genotyping on Wikipedia

::

   Genotyping provides a measurement of the genetic variation between
   members of a species. Single nucleotide polymorphisms (SNP) are one
   of the most common types of genetic variation. A SNP is a single
   base pair mutation at a specific locus, usually consisting of two
   alleles (where the rare allele frequency is< 1%). SNPs are often
   found to be the etiology of many human diseases and are becoming of
   particular interest in pharmacogenetics. Because SNPs are
   evolutionarily conserved, they have been proposed as markers for
   use in quantitative trait loci (QTL) analysis and in association
   studies in place of microsatellites.

On the other hand, genotyping is an experimental procedure and,
typically, there will be SPN with unclear results. It is, however,
possible to use further information -- e.g., the pedigree graph -- to
reconstruct the missing SNP reads.



 1. Collect individuals by study
 2. Select relevant pedigrees
 3. Plan optimal computational strategy
 4. Record results as actions


Collect individual by study
---------------------------

.. code-block:: Python

   import bl.lib.genotype.kb as kb
   import bl.lib.genotype.pedigree as ped

   people = kb.get_individuals_in_study(study)
   print 'genotyping stats:'
   snp_array = kb.get_spn_array('AFFYMETRIX_6.0')
   for p in people:
     if kb.genotyped_on_array(p, snp_array):
       g = kb.get_genotype(p, snp_array)[0]
       if g.missing_calls > eps * len(snp_array):
         broken.append(p)
   founders, non_founders, couples, children = ped.analyze(people)
   families = ped.split_disjoint(ped.propagate_family(broken, children),
                                 children)
   
  
Select relevant pedigrees
-------------------------

.. code-block:: Python

   # all of the following can be written in a functional programming
   # way...

   fam_by_bc = {} 
   for f in families: 
     bc = ped.compute_bit_complexity(f) fam_by_bc.setdefault(bc, []).append(f)
   
   fams = []
   for k in fam_by_bc.keys():
     if k <= bc_max:
       fams.extend(fam_by_bc[k])
     else:
       for f in fam_by_bc[k]:
         critical = set(f).intersect(set(broken))
         fams.extend(ped.grow_family(critical, bc_max))
   #
   # what happens if a critical is in two or more genotypes?
   #


Plan Optimal Computational Strategy
-----------------------------------


.. code-block:: Python

   fam_by_bc = {} 
   for f in fams: 
     bc = ped.compute_bit_complexity(f) fam_by_bc.setdefault(bc, []).append(f)
   real_estate, submission_seq = work_order(fams)
   #
   # Below here is ped file preparation and hadoop job submission

   
   
   
   


   



 
