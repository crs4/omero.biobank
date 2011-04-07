"""

Basic Computations on Genotype data
===================================

This example describes how one can use the ``bl.vl.genotype```package
to perform basic computation on genotype data.

...


"""

import bl.lib.pedal.io as io
from bl.vl.genotype.kb import KnowledgeBase as gKB
from bl.vl.genotype import algo
import numpy as np

import os
import time
import itertools as it
import logging
logging.basicConfig(level=logging.DEBUG)

def get_markers_set_vid(kb, maker, model):
  res = kb.get_snp_markers_sets(selector='(maker=="%s")&(model=="%s")' % (maker, model))
  return res[0]['vid']

def main():
  OME_HOST = os.getenv("OME_HOST", "localhost")
  OME_USER = os.getenv("OME_USER", "root")
  OME_PASS = os.getenv("OME_PASS", "romeo")

  gkb = gKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)

  #--
  maker, model = 'crs4-bl', 'taqman-foo'
  set_vid = get_markers_set_vid(gkb, maker, model)
  s = gkb.get_gdo_iterator(set_vid)
  start = time.clock()
  counts = algo.count_homozygotes(s)
  print 'counts on %d:' % counts[0], time.clock() - start
  start = time.clock()
  mafs = algo.maf(None, counts)
  print 'mafs on %d:' % counts[0], time.clock() - start
  start = time.clock()
  hwe  = algo.hwe(None, counts)
  print 'hwe on %d:' % counts[0], time.clock() - start
  #--

main()
