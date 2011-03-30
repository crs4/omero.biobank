"""

Create SNP Tables
=================

This is a dangerous thing to do. It is destructive, and it should be
done only when all tables need to be redefined.
"""

import bl.lib.genotype.kb.drivers.omero.table_ops as otop
import bl.lib.genotype.kb.drivers.omero.markers as okbm

import os
import logging
logging.basicConfig(level=logging.DEBUG)

def main():
  OME_HOST = os.getenv("OME_HOST", "localhost")
  OME_USER = os.getenv("OME_USER", "root")
  OME_PASS = os.getenv("OME_PASS", "romeo")
  otop.delete_table(OME_HOST, OME_USER, OME_PASS,
                    okbm.SNP_DEFINITION_TABLE)
  otop.create_snp_definition_table(OME_HOST, OME_USER, OME_PASS,
                                   okbm.SNP_DEFINITION_TABLE)
  #-
  otop.delete_table(OME_HOST, OME_USER, OME_PASS, okbm.SNP_ALIGNMENT_TABLE)
  otop.create_snp_alignment_table(OME_HOST, OME_USER, OME_PASS,
                                  okbm.SNP_ALIGNMENT_TABLE)
  #-
  otop.delete_table(OME_HOST, OME_USER, OME_PASS, okbm.SNP_SET_DEF_TABLE)
  otop.create_snp_set_def_table(OME_HOST, OME_USER, OME_PASS,
                                okbm.SNP_SET_DEF_TABLE)
  #-
  otop.delete_table(OME_HOST, OME_USER, OME_PASS, okbm.SNP_SET_TABLE)
  otop.create_snp_set_table(OME_HOST, OME_USER, OME_PASS,
                            okbm.SNP_SET_TABLE)

main()
