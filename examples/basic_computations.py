# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""

Basic Computations on Genotyping data
======================================

This example describes how one can use  the ``bl.vl.genotype```package
to perform basic computation on genotype data.

...


"""

import sys, argparse, time

from bl.core.utils import NullLogger
from bl.vl.utils import LOG_LEVELS, get_logger
from bl.vl.kb import KnowledgeBase as KB
from bl.vl.genotype import algo


def make_parser():
  parser = argparse.ArgumentParser(description="Basic computations example")
  parser.add_argument('--logfile', metavar="FILE",
                      help='log file, defaults to stderr')
  parser.add_argument('--loglevel', metavar="STRING",
                      choices=LOG_LEVELS, help='logging level', default='INFO')
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


class App(object):

  def __init__(self, host, user, passwd, logger=None):
    self.kb = KB(driver='omero')(host, user, passwd)
    self.logger = logger or NullLogger()

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
  if not (args.passwd and args.maker and args.model and args.release):
    parser.print_help()
    sys.exit(1)
  logger = get_logger("main", level=args.loglevel, filename=args.logfile)
  app = App(args.host, args.user, args.passwd, logger=logger)
  app.compute(args.maker, args.model, args.release)


if __name__ == "__main__":
    main()
