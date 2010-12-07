import os, sys, math, optparse
import numpy as np

from mpl_toolkits.mplot3d.axes3d import Axes3D
import matplotlib.pyplot as plt

import bl.lib.genotype.pedigree as ped


CHR_INFO = [
  #N  Ngene  Nbases     Nbases seq
  [1, 4220,  247199719, 224999719],
  [2, 1491,  242751149, 237712649],
  [3, 1550,  199446827, 194704827],
  [4, 446,   191263063, 187297063],
  [5, 609,   180837866, 177702766],
  [6, 2281,  170896993, 167273993],
  [7, 2135,  158821424, 154952424],
  [8, 1106,  146274826, 142612826],
  [9, 1920,  140442298, 120312298],
  [10, 1793, 135374737, 131624737],
  [11, 379,  134452384, 131130853],
  [12, 1430, 132289534, 130303534],
  [13, 924,  114127980,  95559980],
  [14, 1347, 106360585,  88290585],
  [15, 921,  100338915,  81341915],
  [16, 909,   88822254,  78884754],
  [17, 1672,  78654742,  77800220],
  [18, 519,   76117153,  74656155],
  [19, 1555,  63806651,  55785651],
  [20, 1008,  62435965,  59505254],
  [21, 578,   46944323,  34171998],
  [22, 1092,  49528953,  34893953],
  ## [23, 1846, 154913754, 151058754], # X
  ## [24, 454,   57741652,  25121652], # Y
  ]

CHR_SIZE={}
for l in CHR_INFO:
  k = l[0]
  CHR_SIZE[k] = l[2]

RUN_SIZE_CUTOFF =  4
RUN_TIME_SCALE = 10
RUN_ALPHA = 0.535

N_CORES = 8


def parse_run_data(run_data_fn):
  data = {}
  f = open(run_data_fn)
  for line in f:
    line = line.strip()
    if line:
      chr, bc, t = map(float, line.split())
      #data[(chr, bc)] = t
      chr_data = data.setdefault(chr, {})
      chr_data[bc] = t
  return data


def fit_exp(x, y, offset=0):
  """
  Find f(x)=A*exp(K*x)+offset that fits (x, y).
  """
  y = y - offset
  y = np.log(y)
  K, logA = np.polyfit(x, y, 1)
  A = math.exp(logA)
  return lambda x: A*math.exp(K*x)+offset


class RunDataFitter(object):

  def __init__(self, run_data):
    self.run_data = run_data
    self.fitting_functions = self.__get_fitting_functions()

  def __get_fitting_functions(self):
    ff = {}
    for chr, chr_data in self.run_data.iteritems():
      complexities, timings = map(np.array, zip(*sorted(chr_data.iteritems())))
      ff[chr] = fit_exp(complexities, timings)
    return ff
    

def time_of_run(cb, chr, data_fitter=None):
  if data_fitter is not None:
    ff = data_fitter.fitting_functions[chr]
    return ff(cb)
  cb = max(RUN_SIZE_CUTOFF, cb)
  time_scale = RUN_TIME_SCALE * float(CHR_SIZE[chr])/CHR_SIZE[1]
  return time_scale * math.exp(RUN_ALPHA*cb)


class individual(object):
  def __init__(self, iid, sex, father=None, mother=None, genotyped=False):
    self.id = iid
    self.sex = sex
    self.father = father
    self.mother = mother
    self.genotyped = genotyped


def read_ped_file(pedfile):
  fin = open(pedfile)
  inds = {}
  for l in fin:
    l = l.strip()
    if len(l) == 0:
      continue
    fields = l.split()
    fam_label, label, father, mother, sex, genotyped = fields
    genotyped = genotyped != '0'
    inds[label] = individual(label, sex, father, mother, genotyped)
  fin.close()
  for label, ind in inds.iteritems():
    inds[label].father = inds.get(ind.father, None)
    inds[label].mother = inds.get(ind.mother, None)
  return inds.values()


def break_sub_families(family, max_complexity):
  founders, non_founders, couples, children = ped.analyze(family)
  splits = ped.split_disjoint(family, children)
  fams = []
  for f in splits:
    cbn = ped.compute_bit_complexity(f)
    if cbn > max_complexity:
      subfs = ped.split_family(f, max_complexity)
      fams.extend(subfs)
    else:
      fams.append(f)
  return fams


class Cluster(object):
  
  def __init__(self, n):
    self.node_horizon = 0 * np.array(range(n))

  def submit(self, t):
    inode = np.argmin(self.node_horizon)
    self.node_horizon[inode] += t

  def horizon(self):
    inode = np.argmax(self.node_horizon)
    return self.node_horizon[inode]


def time_of_job(histo, n_nodes, data_fitter=None):
  cluster = Cluster(n_nodes)
  cbs = histo.keys()
  cbs.sort(reverse=True)
  for k in cbs:
    for i in range(histo[k]):
      for c in CHR_SIZE.keys():
        T = time_of_run(k, c, data_fitter)
        cluster.submit(T)
  return cluster.horizon()


def get_optimal_curve(X, Y, Z):
  x_idx = np.arange(len(X), dtype=np.int32)
  y_idx = ((Z[x_idx,0:-1] - Z[x_idx,1:]) > 0).sum(axis=1)
  z_opt = Z[(x_idx, y_idx)]
  x_opt = X[x_idx]
  y_opt = Y[y_idx]
  return x_opt, y_opt, z_opt


def draw_result(complexity, n_nodes, horizon):
  X, Y = np.meshgrid(n_nodes, complexity)
  Z = np.log10(horizon)
  #Z = horizon
  x_opt, y_opt, z_opt = get_optimal_curve(complexity, n_nodes, Z)
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1, projection='3d')
  ax.plot_wireframe(X, Y, Z)
  ax.set_xlabel('Cluster Nodes')
  ax.set_ylabel('Max Complexity')
  ax.set_zlabel('Duration[sec] (log10)')
  ax.plot(y_opt, x_opt, z_opt,
          color='r',
          label='optimal number of nodes')
  plt.savefig('plan.png')
  plt.show()


def make_parser():
  parser = optparse.OptionParser(usage="%s [OPTIONS] PED_FILE")
  #parser.set_description(__doc__.lstrip())
  parser.add_option("--run-data", type="str", metavar="STRING",
                    help="file with actual run data in tabular format")
  return parser
  

def main(argv):
  
  parser = make_parser()
  opt, args = parser.parse_args()
  try:
    ped_file = args[0]
  except IndexError:
    parser.print_help()
    sys.exit(2)
  if opt.run_data:
    run_data = parse_run_data(opt.run_data)
    data_fitter = RunDataFitter(run_data)
  else:
    data_fitter = None

  family = read_ped_file(ped_file)

  complexity = range(10, 24)
  n_nodes    = range(5, 400, 5)
  horizon = np.zeros((len(complexity), len(n_nodes)))
  for i, max_complexity in enumerate(complexity):
    fams = break_sub_families(family, max_complexity)
    histo = {}
    for f in fams:
      cb = ped.compute_bit_complexity(f)
      histo[cb] = histo.get(cb, 0) + 1
    for j, n in enumerate(n_nodes):
      print 'doing %d, %d' % (i, j)
      t = time_of_job(histo, n*N_CORES, data_fitter)
      horizon[i,j] = t
  draw_result(np.array(complexity), np.array(n_nodes), horizon)


## python plan_run.py \
##   --run-data ../tests/bl/lib/genotype/data/merlin_timings.tsv \
##   ../tests/bl/lib/genotype/data/ped_soup.ped
if __name__ == "__main__":
  main(sys.argv)
