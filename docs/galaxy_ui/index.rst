Galaxy based User Interface
===========================

Many of omero/vl operations are accessible, and usable, via galaxy.

Developments details are in the developer manual, here we are mainly
concerned with their usage.

This is the list of the currently available modules.

 * Data import into Omero/VL

   - study definitions
   - ...

 * Data analysis

   - 
   -
  
 * Other tools

   - map_vid
   - data tree coverage


Data import
-----------

The general strategy for the import is divided in three steps:

 * upload a file;

 * convert user understandable labels to unique identifies VID using
   ```map_vid```;

 * invoke the actual importer.

Data Analysis
-------------



Other tools
-----------


Data tree coverage
..................

This tool will provide statistics on the data currently known to the
Omero/VL system. 
Selected a study, it will report on:

 * total number of individuals present in the study and their
   distribution by gender and affection status (?)

 * available datasets

This could be structured as a tree::

 study
    - path
         - dataset
                 - gender


Contents:

.. toctree::
   :maxdepth: 2

   
