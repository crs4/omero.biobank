# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""

Basic Computations on Genotyping data
======================================

This example describes how one can use  the ``bl.vl.genotype```package
to perform basic computation on genotype data.

...


"""

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.genotype import algo
import numpy as np

import argparse
import os
import time
import itertools as it
import logging
logging.basicConfig(level=logging.INFO)


#------------------------------------------------------------------------------
def make_parser():
  parser = argparse.ArgumentParser(description="Basic computations example")
  parser.add_argument('--maker', type=str,
                      help='the SNPMarkersSet maker')
  parser.add_argument('--model', type=str,
                      help='the SNPMarkersSet model')
  parser.add_argument('--release', type=str,
                      help='the SNPMarkersSet release')
  parser.add_argument('-H', '--host', type=str,
                      help='omero host system',
                      default='localhost')
  parser.add_argument('-U', '--user', type=str,
                      help='omero user',
                      default='root')
  parser.add_argument('-P', '--passwd', type=str,
                      help='omero user passwd')
  return parser
#------------------------------------------------------------------------------

class App(object):

  def __init__(self, host, user, passwd):
    self.kb = KB(driver='omero')(host, user, passwd)
    self.logger = logging.getLogger()

  def compute(self, maker, model, release):
    mset = self.kb.get_snp_markers_set(maker, model, release)
    if not mset:
      raise ValueError('SNPMarkersSet[%s,%s,%s] has not been defined.'
                       % (maker, model, release))
    # projector = (np.arange(0, 100), np.array([101, 109]), np.arange(110,N))
    # selector = kb.build_selector(
    # s = self.kb.get_gdo_iterator(mset, selector, projector)
    s = self.kb.get_gdo_iterator(mset)
    #--
    start = time.clock()
    counts = algo.count_homozygotes(s)
    print 'counts on %d:' % counts[0], time.clock() - start
    start = time.clock()
    mafs = algo.maf(None, counts)
    print 'mafs on %d:' % counts[0], time.clock() - start
    start = time.clock()
    hwe  = algo.hwe(None, counts)
    print 'hwe on %d:' % counts[0], time.clock() - start


def main():
  parser = make_parser()
  args = parser.parse_args()
  if not (args.passwd
          and args.maker and args.model and args.release):
    parser.print_help()
    sys.exit(1)

  app = App(args.host, args.user, args.passwd)
  app.compute(args.maker, args.model, args.release)

if __name__ == "__main__":
    main()
