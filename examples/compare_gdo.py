"""

Compare GDO for the same individual
===================================

This example describes how one can use the ``bl.vl.genotype```package
to perform a comparison of genotyping results, obtained with different
technologies, for the same individual.

In this example we are assuming that FIXME


"""

from bl.lib.individual.kb import KnowledgeBase as iKB
from bl.lib.genotype.kb   import KnowledgeBase as gKB
from bl.lib.genotype      import algo
import numpy as np

import os
import time
import itertools as it
import logging
logging.basicConfig(level=logging.DEBUG)

def compare_dsets(gdos):
  """
  Compute the average and variance of the per marker SNP probs on the
  intersection of supports of the gdos.
  """
  support, mapping = algo.find_shared_support(gdos)
  all_data = [np.vstack([g['probs'][:,i], g['confs'][:,i]])
              for (g, i) in it.izip(gdos,mapping)]
  map(lambda _ : np.reshape(_, (1,) + _.shape), all_data)
  all_data = np.vstack(all_data)
  v  = all_data[:,0:2,:].sum(axis=0)
  v2 = (all_data[:,0:2,:]**2).sum(axis=0)
  N = all_data.shape[0]
  mean = v/N
  # FIXME: I know, this is not the variance...
  var = v2/N - mean**2
  return (support, mean, np.sqrt(var))


def main():
  OME_HOST = os.getenv("OME_HOST", "localhost")
  OME_USER = os.getenv("OME_USER", "root")
  OME_PASS = os.getenv("OME_PASS", "romeo")

  ikb = iKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)
  gkb = gKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)
  #--
  inds = ikb.get_individuals(study='foo')
  #--
  for i in inds:
    dsets = gkb.get_gdos(i.id)
    support, mean, sigma = compare_dsets(dsets)

main()
