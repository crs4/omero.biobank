
SNP markers management in VL
============================

The following is a list of the typical steps. 

#. A SNP is defined by its mask sequence. We will follow the
   convention that a mask is written as::

    GGATACATTTTATTGC[A/G]CTTGCAGAGTATTTTT

   with the mask subdivided in two flanking regions and the possible
   allele values at the SNP position.  We will follow the convention
   that the first allele in the pair is allele A and the second allele
   B.  The sequence is supposed to be **ALWAYS** in the TOP strand as
   defined by Illumina conventions. Specific genoytping technology
   related swaps between allele definitions will be handled in the
   appropriate MarkersSet definitions, see below.

#. All SNP definitions are saved in a central table with the following
   columns::

    vid        source     context label         rs_label  mask                                  op_vid
    V00909032  affymetrix gene6.0 SNP_A-1780419 rs6576700 GGATACATTTTATTGC[A/G]CTTGCAGAGTATTTTT V233903
    V00909032  affymetrix gene6.0 SNP_A-1780419 rs6576700 GGATACATTTTATTGC[A/G]CTTGCAGAGTATTTTT V233903
    ...
   
   where the ``op_vid`` column stores the vid of the last operation that touched this row.

#. To be useful, SNPs need to be aligned to a reference genome. This
   information is saved in another table with the following columns::

    marker_vid ref_genome chromosome pos    global_pos  strand allele copies op_vid
    V902439090 hg18       2          492589 20000492589 T      A      1      V9024398989
    V902439092 hg18       1          493589 20000492589 T      A      2      V9024398989
    V902439092 hg18       1          493899 20000492899 F      B      2      V9024398989
    ...

   Where strand is True if the alignment is on the same strand of the
   reference, while pos is the position of the SNP and allele is the
   matched allele at that position. Note that we are using the A/B
   convention, so the actual base seen at that position needs to be
   inferred by the SNP mask in the definition table. The rationale is
   that all the information is relative to a line in the alignment
   table. If there are multiple hits, there will be multiple rows, but
   each row will have the copies column set to the number of hits.
   SNPs that failed to align unambiguously will have a row with the
   chromosome, position and number of copies set to 0.
   The align_vid column keeps track of the operation that generated
   the aligment.  It is unclear if we want to mantain more labeling
   information...  The global_pos column provides a sortable
   coordinate for the SNP position. It is currently defined as::
   
     global_pos = chromosome * MAX_GENOME_LEN + pos

   where chromosome should be one of range(1, 25) and 23 (X), 24 (Y)
   and 25(MT).

#. Markers are usually collected in sets corresponding, for instance,
   to the markers used by a specific genotyping technology::

    vid      maker       model  marker_vid  marker_indx allele_flip op_vid
    V030303  affymetrix  GW6.0  V902439090  0           T           V8398989
    V030303  affymetrix  GW6.0  V902439093  1           F           V8398989
    ...

   Where marker_indx is the relative ordered position of the marker
   within the marker set, and allele_flip records if the specific
   genotyping technology uses a reverse convention on A/B allele
   naming with respect to the marker definition table.

The general strategy to define new markers is the following:

#. Acquire the list of markers for a given technology

#. Use snp_manager to find out if they correspond to known dbSNP
   SNPs. If they do, register a line such as the following one in omero::

     source context label rs_label mask
     dbSNP  build36 rs791 rs791    AAGGAAGGAGCTGGANTTTTTTT[A/T]AAATCTTTCTTTCTGGTGTTTAA

   The actual mapping from the specific technology SNPs to the VL
   markers definition is delegated to the creation of the SNPMarkersSet.

   If they do not, register a line such as the following one instead::

     source     context label         rs_label mask
     affymetrix gene6.0 SNP_A-1780419 None     GGATACATTTTATTGC[A/G]CTTGCAGAGTATTTTT


Supported operations
--------------------

FIXME: this section should actually go in the Developer's Manual. All
bulk markers import operations are currently supported by the importer
tool.

 1. Upload a new set of markers.
 2. Align markers to a new reference genome.
 3. Order a set of markers by their coordinates.



Upload a new set of markers
,,,,,,,,,,,,,,,,,,,,,,,,,,,

.. code-block:: python

   import bl.vl.kb as kb
   import csv

   fname = 'new_markers.tsv'
   i_csv = csv.DictReader(open(fname), delimiter='\t')

   kb.open(HOST, USERNAME, PASSWD)
   kb.extend_snp_definition_table(i_csv, op_vid='V90303')
   kb.close()

   
Align markers to a new reference genome
,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,

.. code-block:: python

   import bl.lib.kb as kb

   kb.open(HOST, USERNAME, PASSWD)
   mrks = kb.open_marker_definition_stream(source='affymetrix', context='GW6.0')

   # we could directly interface to libbwa
   # but this is ok for now.
   fo = open("tmp.tsv", "w")
   fo.write('\t'.join(['']))
   for r in mrks:
     fo.write('\t'.join([]))
   os.system('realing_snp -i tmp.tsv -o foo.tsv -r hg19')
   i_csv = csv.DictReader(open("foo.tsv"), delimiter='\t')
   kb.extend_snp_alignment_table(i_csv, ref_genome="hg19", op_vid)
   kb.close()
  

Order a set of markers by their coordinates
,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,

.. code-block:: python

   import bl.lib.kb as kb

   kb.open(HOST, USERNAME, PASSWD)

   mrks_ids = kb.get_snp_set(maker='affymetrix', model='GW6.0')
   mrks_aligns  = kb.get_snp_positions(mrks_ids, ref_genome='hg19', copies=1)
   # canonical sorting
   mrks_alings.sort(order=['chromosome', 'position'])
   # a possibly faster way
   mrks_alings.sort(order=['global_pos'])


Actual implementation
---------------------

In the ``examples`` directory you can find a working
implementation of what was described above:

FIXME: make the definition titles actual links.

``load_genotypes.py``

     a script that will load a collection of genotypes

``basic_computations.py``

     a script that will show how to do basic computations on the
     previously loaded genotypes.
