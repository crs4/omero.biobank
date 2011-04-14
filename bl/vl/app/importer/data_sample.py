"""
Import of DNA samples
=====================

Will read in a csv file with the following columns::

  bslabel bsbarcode dnalabel dnabarcode initial_volume current_volume \
     nanodrop_concentration qp230260 qp230280 status
  bs01    328989238 dn01      3989328   20             20            USABLE
  bs03    328989228 dn02      2389898   20             20            USABLE
  ....

Record that point to an unknown (bslabel, bsbarcode) pair will be noisily
ignored. The same will happen to records that have the same dnalabel or
dnabarcode of a previously seen dna sample.
"""
