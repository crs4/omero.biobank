openEHR support
===============

Tutorial
--------

.. code-block:: python

  >>> inds = kb.get_objects(kb.Individual)
  >>> ehr = kb.get_ehr(i)
  >>> for x in ehr.recs[r]:
        print x['timestamp'], x['archetype'], x['fields']
  1310057541700 openEHR-EHR-EVALUATION.problem-diagnosis.v1 {'at0002.1': 'icd10-cm:G35'}
  1310057541608 openEHR-EHR-EVALUATION.problem-diagnosis.v1 {'at0002.1': 'icd10-cm:E10'}
  1310057541700 openEHR-EHR-EVALUATION.problem-diagnosis.v1 {'at0002.1': 'icd10-cm:E10'}
  >>> ehr.matches('openEHR-EHR-EVALUATION.problem-diagnosis.v1')
  True
  >>> ehr.matches('openEHR-EHR-EVALUATION.problem-diagnosis.v1', 'at0002.1')
  True
  >>> ehr.matches('openEHR-EHR-EVALUATION.problem-diagnosis.v1', 'at0002.1', 'icd10-cm:E10')
  True
  >>> ehr.matches('openEHR-EHR-EVALUATION.problem-diagnosis.v1', 'at0002.1', 'icd10-cm:G35')
  True
  >>> ehr.matches('openEHR-EHR-EVALUATION.problem-diagnosis.v1', 'at0002.1', 'icd10-cm:XXX')
  False



Supported openEHR archetypes
----------------------------

Archetype exclusion-problem_diagnosis
.....................................


This archetype is mainly used to mark control individuals.  It
corresponds, of course, only to a probable situation at a given time
point, thus an actual selection of, say, control individuals should
not simply search for this archetype but fetch

FIXME add the archetype ADL::

  openEHR-EHR-EVALUATION.exclusion-problem_diagnosis.v1
  {'at0002.1' : 'local:at0.3' } # No significant medical history

This will be the default setting. Of course, if the evaluation is done
correctly we could have something less generic and, most likely, more
accurate.

Archetype problem-diagnosis
,,,,,,,,,,,,,,,,,,,,,,,,,,,


This archetype is used to 

FIXME add the archetype ADL::
  
  openEHR-EHR-EVALUATION.problem-diagnosis.v1
  fields := {'at0002.1' : term}
  term := terminology:code # e.g., icd10-cm:E10 (Type 1 Diabetes)


Mapping OpenEHR archetypes to omerotables
-----------------------------------------

We use a flat representation of FIXME

Mapping of concrete data types
..............................

A possible strategy is to have an EAV row that will have the following columns::

  bool
  long
  float
  DV_CODED_TEXT  [terminology:value]
  DV_QUANTITY    split in three columns: units (str)      [e.g., 'mm[Hg]']
                                         magnitude(double) [e.g., 120.0]
                                         precision(float) [e.g., 0.2]

units is expressed using the units syntax described in OpenEHR Data Types Information Model v1.0.2, section 6.2.8.

The obvious thing to do would be to use stuctured 'columns'

Let us now discuss this strategy in more detail.




DV_CODED_TEXT
,,,,,,,,,,,,,

OpenEHR Data Types Information Model v1.0.2
5.2.4 DV_CODED_TEXT Class. 




DV_QUANTITY
,,,,,,,,,,,

OpenEHR Data Types Information Model v1.0.2
6.2.7 DV_QUANTITY Class::


  ELEMENT[at0004] occurrences matches {0..1} matches {	-- Systolic
   value matches {
       C_DV_QUANTITY <
	property = <[openehr::125]>
	list = <
                ["1"] = <
		units = <"mm[Hg]">
   	        magnitude = <|0.0..<1000.0|>
  	        precision = <|0|>			
		>
 	       >
       >
   }
  }
