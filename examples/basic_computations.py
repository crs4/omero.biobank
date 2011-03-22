"""

Basic Calculations on Genotype data
===================================

In this example we are assuming that


"""

import bl.lib.pedal.io as io
from bl.lib.genotype.kb import KnowledgeBase
from bl.lib.genotype import algo
import numpy as np

import os
import itertools as it
import logging
logging.basicConfig(level=logging.DEBUG)

def main():
  OME_HOST = os.getenv("OME_HOST", "localhost")
  OME_USER = os.getenv("OME_USER", "root")
  OME_PASS = os.getenv("OME_PASS", "romeo")

  kb = KnowledgeBase(driver='omero')

  kb.open(OME_HOST, OME_USER, OME_PASS)
  #--
  maker, model = 'crs4-bl', 'taqman-foo'
  set_vid = kb.get_snp_marker_set_vid(maker, model)
  s = kb.get_gdo_stream(set_vid)
  counts = algo.count_homozygotes(s)
  mafs = algo.maf(None, counts)
  hwe  = algo.hwe(None, counts)
  #--
  kb.close()

main()
