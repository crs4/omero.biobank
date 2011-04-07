"""

Create SNP Tables
=================

This is a dangerous thing to do. It is destructive, and it should be
done only when all tables need to be redefined.
"""


from bl.vl.genotype.kb.drivers.omero.proxy import Proxy

import os
import logging
logging.basicConfig(level=logging.DEBUG)


def main():
  OME_HOST = os.getenv("OME_HOST", "localhost")
  OME_USER = os.getenv("OME_USER", "root")
  OME_PASS = os.getenv("OME_PASS", "romeo")

  #-- these are low level operations needed to setup the environment
  p = Proxy(OME_HOST, OME_USER, OME_PASS)
  p.delete_table(Proxy.SNP_MARKER_DEFINITIONS_TABLE)
  p.create_snp_marker_definitions_table()
  #-
  p.delete_table(Proxy.SNP_ALIGNMENT_TABLE)
  p.create_snp_alignment_table()
  #-
  p.delete_table(Proxy.SNP_SET_DEF_TABLE)
  p.delete_table(Proxy.SNP_SET_TABLE)
  p.create_snp_markers_set_table()
  p.create_snp_set_table()

main()
