How to import data into VL
==========================

General strategy
----------------

The import operations can almost always be descrived as:

  * reading a tsv input file with one column named '''label''', an
    unique id for the specific object defined, and other fields, when
    the specific object is linked to another, e.g., a blood sample to
    an individual, there will be one column named '''source''' with the
    vid of an object to which this object should be linked to;

  * saving the information there contained, plus other data provided
    as parameters, the record and the context should provide enough
    information to be able to generate, together with the object, also
    the relavant action linking the saved object to the source
    specified in the '''source''' column;

  * outputting an object mapping tsv file with four columns,
    '''study''', '''label''', '''object_type''', '''vid''', where vid
    is the unique VL id assigned by Omero/VL to the object, and
    object_type is Omero/VL object type of the object.

The mapping from user known source labels to vid is the user
responsibility, the typical procedure is to use utility tools such as
FIXME to query the KB and obtain the needed vid.


Importing a Study definition
----------------------------

A study is essentially a label to represent a general context. 
It is characterized by the following fields::
  
  label  description
  ASTUDY A textual description of ASTUDY, no tabs please.


The description column is optional. It will be filled with the string
'''No description provided'''.

.. code-block:: bash

   ${IMPORT} ${SERVER_OPTS} study --label A_NEW_STUDY


Importing an Individual definition
----------------------------------

An individual is characterized by the following fields::

  study label gender   father mother
  xxx   id2   male     None   None
  xxx   id2   male     id4    id5

where gender could be either male or female, father and mother could
either be the string '''None''' or a label (within the same study)
individual.  Individuals are the only "bio" objects in Omero/VL that
can be loaded independently from what is already in the DB. Of course,
if they do not have assigned parents.

.. code-block:: bash
   
    ${IMPORT} ${SERVER_OPTS} -i individuals.tsv individual -S ${DEFAULT_STUDY}

**NOTE:** The current incarnation of the import application does not support
cross studies parenthood definitions.


Importing a BioSample definition
--------------------------------

A biosample record will have, at least, the following fields::

  label     source    
  I001-bs-2 V932814892
  I002-bs-2 V932814892

Where '''label''' is the label of the biosample container

This is not enough to provide the minimum set of required information,
and they should be provided as command line options.

.. code-block:: bash
   
   ${IMPORT} ${SERVER_OPTS} -i bio_samples.tsv 
                            -o bio_mapping.tsv biosample \
                            --study  ${DEFAULT_STUDY} \
                            --container-type Tube \
                            --source-type Individual \
                            --container-content BLOOD \
                            --container-status  USABLE \
                            --current-volume 20
			    
where content is taken from the enum VesselContent possible values and
status from the enum VesselStatus

Another example, this time dna samples::

  label    source     container_content used_volume current_volume
  I001-dna V932814899 DNA               0.3         0.2
  I002-dna V932814900 DNA               0.22        0.2

where '''used_volume''' and '''current_volume''' are measured in FIXME
microliters.

.. code-block:: bash
   
   ${IMPORT} ${SERVER_OPTS} -i bio_samples.tsv 
                            -o bio_mapping.tsv biosample \
                            --study  ${DEFAULT_STUDY} \
                            --container-type Tube \
                            --source-type Tube \
                            --container-status  USABLE


FIXME: provide detailed list of possible columns, their accepted
values and the implied importing rules. Discuss also the meaning of
'''used_volume'''.

A special case is when the records refer to biosamples contained in
plate wells. Together with the minimal columns above, there should be
a column with the vid of the relevant TiterPlate (see below). For instance::

  plate  label source
  V39030 A01   V932814892
  V39031 A02   V932814893
  V39032 A03   V932814894

where the label column is now the label of the well position. 


Importing a TiterPlate
----------------------

A full TiterPlate records will have the following columns::

  study  label   barcode rows columns plate_status maker model
  ASTUDY p090    2399389 32   48      xxxx  yyy

The plate_status, maker and model columns are optional, as well as the
barcode one.  Default plate dimensions can be provided with a flag

.. code-block:: bash

   ${IMPORT} ${SERVER_OPTS} -i titer_plates.tsv
                            -o titer_plates_mapping.tsv\
                            titer_plate\
                            --study  ${DEFAULT_STUDY}\
                            --plate-shape=32x48\
                            --maker=foomaker\
                            --model=foomodel

where container-content is taken from the enum VesselContent possible
values and container-status from the enum VesselStatus


Importing a PlateWell
---------------------

Will read in a csv file with the following columns::

  study plate_label label row column source used_volume current_volume
  ASTDY p01         J01   10  1      V93090 0.1         0.1
  ASTDY p01         J02   10  2      V90020 0.1         0.1

Each row will be interpreted as follows.
Add a PlateWell to the plate identified by plate_label, The PlateWell
will have, within that plate, the unique identifier label. If row and
column (optional) are provided, it will use that location. If they are
not, it will deduce them from label (e.g., J01 -> row=10,
column=1). Missing labels will be generated as

       '%s%03d' % (chr(row + ord('A') - 1), column)

Badly formed label will bring the rejection of the record. The same
will happen if label, row and column are inconsistent.  The well will
be filled by current_volume material produced by removing used_volume
material taken from the bio material contained in the vessel
identified by bio_sample_label. Row and Column are base 1.

FIXME: currently there is no way to specialize the action performed,
it will always be marked as an ActionCategory.ALIQUOTING.



Data Samples
------------


Data Objects
------------






Each step of the import process will output a file with the mapping
from the **external** label of




See ``tests/tools/importer`` for examples of input data files.

FIXME: add descriptions.

Common variables
----------------

.. code-block:: bash

    #!/bin/bash

    DEFAULT_STUDY=ILENIA3.41
    TDIR=~/svn/bl/vl/trunk/tools
    IMPORT="python ${TDIR}/importer"
    SERVER_OPTS="-H biobank05 -U root -P romeo"


Create SNP tables
-----------------
    
***NOTE:*** this operation is DESTRUCTIVE -- Activate it by adding ``--do-it``

.. code-block:: bash

    python ${TDIR}/create_snp_tables ${SERVER_OPTS} #--do-it

    
Import individuals
------------------

.. code-block:: bash
   
    ${IMPORT} ${SERVER_OPTS} -i individuals.tsv individual -S ${DEFAULT_STUDY}


Import biosamples
-----------------

.. code-block:: bash
    
    ${IMPORT} ${SERVER_OPTS} -i samples_blood_samples.tsv blood_sample \
      -S ${DEFAULT_STUDY}
    ${IMPORT} ${SERVER_OPTS} -i samples_dna_samples.tsv dna_sample \
      -S ${DEFAULT_STUDY}


Plate preparation
-----------------

.. code-block:: bash

    ${IMPORT} ${SERVER_OPTS} -i samples_titer_plates.tsv titer_plate \
      -S ${DEFAULT_STUDY}
    ${IMPORT} ${SERVER_OPTS} -i samples_plate_wells.tsv plate_well \
      -S ${DEFAULT_STUDY}


Import clinical data
--------------------

.. code-block:: bash

    ${IMPORT} ${SERVER_OPTS} -i samples_diagnosis.tsv diagnosis


Import digital data
-------------------

.. code-block:: bash

    ${IMPORT} ${SERVER_OPTS} -i samples_devices.tsv device
    ${IMPORT} ${SERVER_OPTS} -i samples_affy_gw.tsv data_sample


Import marker definitions
-------------------------

FIXME the importer will convert the input marker definition sequences
Illumina convention TOP strand FIXME ref. illumina techrep.

.. code-block:: bash

    ${IMPORT} ${SERVER_OPTS} -i Affymetrix_GenomeWideSNP_6_na28.tsv \
      marker_definition --source='Affymetrix' --context='GenomeWide-6.0' \
      --release='na28' -S ${DEFAULT_STUDY}


Import data objects
-------------------

Input tsv files are generated by ``chipal import_data``\ .
FIXME: cross-reference chipal docs.

.. code-block:: bash

    ${IMPORT} ${SERVER_OPTS} -i old_to_cels_20110704.tsv data_object \
      -S ${DEFAULT_STUDY}


Computing with omero/vl
-----------------------

Extract marker data for mapping to ref genome
FIXME: add more steps

.. code-block:: bash

    python ${TDIR}/kb_query -o markers.tsv ${SERVER_OPTS} markers \
      --definition-source="(Affymetrix,GenomeWide-6.0,na28)" \
      --fields-set=mapping
