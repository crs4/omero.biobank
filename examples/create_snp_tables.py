"""

Create SNP Tables
=================

This is a dangerous thing to do. It is destructive, and it should be
done only when all tables need to be redefined.
"""

import bl.lib.genotype.kb.drivers.omero.utils   as okbu
import bl.lib.genotype.kb.drivers.omero.markers as okbm


import os
import logging
logging.basicConfig(level=logging.DEBUG)

def main():
  OME_HOST = os.getenv("OME_HOST", "localhost")
  OME_USER = os.getenv("OME_USER", "root")
  OME_PASS = os.getenv("OME_PASS", "romeo")
  okbu.delete_table(OME_HOST, OME_USER, OME_PASS,
                    okbm.SNP_DEFINITION_TABLE)
  okbu.create_snp_definition_table(OME_HOST, OME_USER, OME_PASS,
                                   okbm.SNP_DEFINITION_TABLE)
  #-
  okbu.delete_table(OME_HOST, OME_USER, OME_PASS, okbm.SNP_ALIGNMENT_TABLE)
  okbu.create_snp_alignment_table(OME_HOST, OME_USER, OME_PASS,
                                  okbm.SNP_ALIGNMENT_TABLE)
  #-
  okbu.delete_table(OME_HOST, OME_USER, OME_PASS, okbm.SNP_SET_TABLE)
  okbu.create_snp_set_table(OME_HOST, OME_USER, OME_PASS,
                            okbm.SNP_SET_TABLE)

main()
