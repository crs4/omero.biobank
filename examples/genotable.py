"""

The goal is to have a computationally efficient and reasonably clean
way to handle genotype data. By "genotype data", I mean:

  (a) the result of an high resolution device acquisition, e.g., what
  you get from an Affy 6.0 and an Illumina Infinium;

  (b) the result of interpolation and extrapolation of genotyping data
  to obtain even higher densities datasets.

A genotype data object GDO is composed by:

  (i)   a list of N markers ids that indentifies each snp in the datasets;

  (ii)  a type(np.zeros((2, N), dtype=np.float32)) array with the
        probabilities of the AA and BB configurations;
  (iii) a type(np.zeros((N,), dtype=np.float32)) array with the call confidence.

We are using a floating point representation, rather than the usual
'two-bits' per snp  representation because we are trying to mantain an
uniform description for both the 'experimental' and the 'computed' snp values.

All GDO coming from the same acquisition technology (both real and
synthetic) share the same marker list.

For the purposes of this discussion, what the markers are is
irrelevant. However, their details are currently handled as records of
a pytable table (omero.grid.table) with many millions of rows.

The typical operations that we expect to perform on the data can be
divided in, at least, three flavours.

 *columnar, per snp, statistics*: that is, use call values for the
  same marker across all the GDO in a given GDO set to compute a snp
  specific value, e.g., minimum allele frequency (MAF) and Hardy
  Weinberg equilibrium tests (HWE).

 *row, per GDO, statistics*: that is, use call values for all the snp
  within a GDO, for instance to count the number of
  (homo/hetero)zygotes snps.

 *group of GDO statistics*: that is, use a set to GDO to compute
  quantities that require to look at the same time in both the row and
  col directions, e.g., computing the Hamming distance (on snps)
  between pairs of GDOs, and using imputation techniques (e.g., with
  Merlin) to convert inter individual relations (pedigree)
  to deduce missing snp values.

Ideally, one would like to have server-side scripts that compute
'per-column' and 'per-row' quantities, and maybe, extract blocks of
GDOs and convert them to the format needed by external tools.  For
instance, saving in an HDFS directory a file with, for each snp, a record
containing the relative genotype for all the individuals in the
selected set.

Things like saving a new GDO will be left to the client, as well
as recovering a GDO.

As an example, below is a simplistic, pytable based, implementation of
a genotypes storage facility and two examples of operations of the first class.

"""

import numpy as np
import tables as tb
import time
from optparse import OptionParser


N=100000 # Just a reasonable test size.
class GDOAffy6_0(tb.IsDescription):
  """

  vid  is a unique id for this GDO
  act  is a unique id of the act that generated this GDO

  probs[0,:] contains prob_AA
  probs[1,:] contains prob_BB

  prob_AB = 1.0 - prob_AA - prob_BB

  confidence comes from the measurement/computation process.

  """
  vid          = tb.StringCol(itemsize=16)
  act          = tb.StringCol(itemsize=16)
  #probs        = tb.Float32Col(shape=(2, N))
  probs        = tb.StringCol(itemsize=(4*2*N))
  confidence   = tb.Float32Col(shape=(N,))

def create_gdos(fname, n_recs=200, p_A=0.3):
  """Just random noise"""
  fh = tb.openFile(fname, mode="w")
  root = fh.root
  for gn in ("GDOs",):
    g = fh.createGroup(root, gn)
  table = fh.createTable('/GDOs', 'affy6_0',
                         GDOAffy6_0,
                         "GDOs Affy6_0")
  sample = table.row
  probs = np.zeros((2,N), dtype=np.float32)
  for i in range(n_recs):
    sample['vid'] = 'V9482948923%05d' % i
    sample['act'] = 'V9482948923%05d' % i
    probs[0,:] = np.random.normal(p_A**2, 0.01*p_A**2, N)
    probs[1,:] = np.random.normal((1.-p_A)**2, 0.01*(1.0-p_A)**2, N)
    sample['probs'] = np.clip(probs, 0, 1.0).tostring()
    sample['confidence'] = np.random.random(N)
    sample.append()
  table.flush()
  fh.close()

#------------------------------------------------------------------------------------------
def count_homozygotes(it):
  """
  Count (compute) the number of  N_AA, N_BB homozygotes.
  """
  setup_done = False
  for i, x in enumerate(it):
    # this is an hack
    probs = np.fromstring(x['probs'], dtype=np.float32).reshape(2, N)
    if not setup_done:
      counts = np.zeros(probs.shape, dtype=np.float32)
      setup_done = True
    counts += probs
  return (i + 1), np.cast[np.int32](counts)

def maf(it, counts=None):
  """
  Compute minor allele frequencies.
  """
  if not counts:
    N, counts = count_homozygotes(it)
  else:
    N, counts = counts
  N_AB = N - counts.sum(axis=0)
  return (2*counts + N_AB)/(2.0*N)

def hwe_probabilites(n_a, n_ab, N):
  """
  Quick and dirty.
  """
  n_a = n_a if n_a <= N else 2*N - n_a
  n_b = 2*N - n_a
  N_ab = np.arange(n_a & 0x01, n_a , 2, dtype=np.float64)
  log_fact = np.log((n_a - N_ab) * (n_b - N_ab) / ((N_ab + 2.0) * (N_ab + 1.0)))
  weight = np.cumsum(log_fact)
  prob = np.exp(weight - weight.max())
  prob /= prob.sum()
  return (prob[N_ab == n_ab], prob)

def hwe_scalar(n_a, n_ab, N):
  p, probs = hwe_probabilites(n_a, n_ab, N)
  return probs[probs <= p].sum()

hwe_vector = np.vectorize(hwe_scalar, [np.float32])

def hwe(it, counts=None):
  if not counts:
    N, counts = count_homozygotes(it)
  else:
    N, counts = counts
  N_AB = N - counts.sum(axis=0)
  N_x = N_AB + 2*counts
  low_freq = N_x.min(axis=0)
  return hwe_vector(low_freq, N_AB, N)

def analize_gdos(fname):
  fh = tb.openFile(fname)
  table = fh.root.GDOs.affy6_0
  start = time.clock()
  counts = count_homozygotes(table.iterrows())
  print 'time spent in count_homozygotes = ', time.clock() - start
  start = time.clock()
  s = maf(None, counts)
  print 'time spent in  maf = ', time.clock() - start
  start = time.clock()
  h = hwe(None, counts)
  print 'time spent in hwe = ', time.clock() - start
  print s[:, 0:10]
  print h[0:10]
  print h.min(), h.max()
  print np.histogram(h, bins=20)

def main():
  p = OptionParser()
  p.add_option("-c", "--create",
               action="store_true", dest="create", default=False,
               help="create h5 file")
  p.add_option("-n", "--n-records", type="int", metavar="INT",
               default=30,
               help="number of records to be created")

  (opts, args) = p.parse_args()
  fname = "gdos.h5"
  if opts.create:
    create_gdos(fname, opts.n_records)
  analize_gdos(fname)

main()



