How to define a SNPMarkersSet
=============================

What is a marker set
--------------------

A **SNPMarkersSet** is, essentially, an ordered list of markers where
the order is, in principle, arbitrary but it usually comes from the
alignment of the SNP markers against a reference genome.  Within VL,
different genotyping technologies are mapped to different
SNPMarkersSet.

In more detail, a marker set is defined by:

 * identification information:

   * **maker:** the name of the organization that has defined the
     SNPMarkersSet, e.g., 'CRS4'

   * **model** the specific "model" of the SNPMarkersSet, e.g.,
     'AffymetrixGenome6.0'

   * **release** a string that identifies this specific instance, e.g.,
     'aligned_on_human_g1k_v37'

 * markers reference list: for each marker that should go in the list,
   the following information is provided:
  
   * **rs_label** the rs identifier of the marker

   * **marker_indx** the position of the marker within the marker set
     list. (Well, a SNPMarkersSet is actually a list, more than a set)

   * **allele_flip** False if the alleles are in the same order as
     recorded in the marker definition, True if they are swapped.

   E.g., ::

    rs_label	marker_indx	allele_flip
    rs12265218	0	False
    rs11981089	1	False
    rs7625376	2	False
    rs741592	3	False
    rs8078079	4	False	
    rs9567736	5	False
    rs4693427	6	False
    rs4757019	7	False
    rs7958813	8	False
   

Creating a new markers set
--------------------------


Creating a new markers set requires the following steps.

 #. create a tsv file with a list of the requested SNPs
 #. (optional) reorder the list against a reference genome
 #. choose a name and a release name
 #. use tools/import to load the SNP markers set into VL


Step 1: create a tsv file
,,,,,,,,,,,,,,,,,,,,,,,,,

Create a tsv file, that we will call `ms_def.tsv`,  with the following columns::

   rs_label	marker_indx	allele_flip
   rs12265218	0	False
   rs11981089	1	False
   rs7625376	2	False
   rs741592	3	False
   rs8078079	4	False	
   rs9567736	5	False
   rs4693427	6	False
   rs4757019	7	False
   rs7958813	8	False

See tests/tools/importer/taq_man_ms_status_markers_set.tsv for an
actual example.
