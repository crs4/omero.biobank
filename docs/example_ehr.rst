Mapping OpenEHR archetypes
==========================

We use a flat representation of 

Mapping of concrete data types
------------------------------

A possible strategy is to have an EAV row that will have the following columns::

  bool
  long
  float
  DV_CODED_TEXT  [terminology:value]
  DV_QUANTITY    split in three columns: units (str)      [e.g., 'mm[Hg]']
                                         magnitude(double) [e.g., 120.0]
                                         precision(float) [e.g., 0.2]

units is expressed using the units syntax described in 6.2.8.

The obvious thing to do would be to use stuctured 'columns'

Let us now discuss this strategy in more detail.




DV_CODED_TEXT
,,,,,,,,,,,,,

OpenEHR Data Types Information Model v1.0.2
5.2.4 DV_CODED_TEXT Class. 




DV_QUANTITY
,,,,,,,,,,,

OpenEHR Data Types Information Model v1.0.2
6.2.7 DV_QUANTITY Class


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

