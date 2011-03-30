"""

Load Markers table
==================

This example shows how one can import markers definition in the knowledge base.

We will be importing affymetrix GenomeWideSNP 6.0 markers definitions
and a small subset of other SNP markers.

"""

import bl.lib.chip.io     as cio
from bl.lib.genotype.kb import KnowledgeBase as gKB

import numpy as np

import os
import itertools as it
import logging
logging.basicConfig(level=logging.DEBUG)

#--------------------------------------------------------------------------------
def import_affy_snp_table(kb):
  affy_snp_tsv = '/data/GenomeWideSNP_6.na28.annot.tsv'
  # FIXME: we need to decide what we want to do with the action vid
  source = 'affymetrix'
  context = 'GenomeWide-6.0-na28'
  op_vid = kb.make_vid()
  tsv = cio.TsvFile(affy_snp_tsv)
  def ns(stream):
    for x in stream:
      r = {'source' : source,
           'context' : context,
           'label'   : x['Probe Set ID'],
           'rs_label' : x['dbSNP RS ID'],
           'mask'    : x['Flank'],
           }
      yield r
  kb.add_snp_marker_definitions(ns(tsv), op_vid=op_vid)

def import_other_snps(kb):
  source  = 'dbsnp'
  context = 'taqman-calibration'
  op_vid = kb.make_vid()
  snps = [{'source' : source, 'context' : context,
           'label' : 'rs12265218', 'rs_label' : 'rs12265218',
           'mask' : 'CACAGTCACTTCTTCAAAGCTTTGCC[A/G]CAGAAGCTACAAGCATTATTTCCAC'},
          {'source' : source, 'context' : context,
           'label' : 'rs11981089', 'rs_label' : 'rs11981089',
           'mask' : 'TACCTTAGGTTCACTAAATATAATAA[C/T]GACATTCAGTACCCAGGAACTACTA'},
          {'source' : source, 'context' : context,
           'label' : 'rs7625376', 'rs_label' : 'rs7625376',
           'mask' : 'AGCCAGTGTCCAGTAGTCCCTGAAAG[A/G]TCTGATTTCCCCCTCCCCTCAGTAC'},
          {'source' : source, 'context' : context,
           'label' : 'rs741592', 'rs_label' : 'rs741592',
           'mask' : 'GGAAGGAAGAAATAAAATATTACATG[C/G]AGAGCATTTCAGCACTATGTCTGGC'},
          ]
  kb.add_snp_marker_definitions(it.islice(snps, len(snps)), op_vid=op_vid)

def main():
  OME_HOST = os.getenv("OME_HOST", "localhost")
  OME_USER = os.getenv("OME_USER", "root")
  OME_PASS = os.getenv("OME_PASS", "romeo")

  gkb = gKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)

  import_affy_snp_table(gkb)
  import_other_snps(gkb)

main()
