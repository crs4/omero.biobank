import numpy as np
import matplotlib.pyplot as plt
import plan_run

data_fn = "../tests/bl/lib/genotype/data/merlin_timings.tsv"
data = plan_run.parse_run_data(data_fn)
data_fitter = plan_run.RunDataFitter(data)
for chr, chr_data in data.iteritems():
  print "doing chr%d" % chr
  plt.clf()
  plt.cla()
  complexities, timings = map(np.array, zip(*sorted(chr_data.iteritems())))
  ff = data_fitter.fitting_functions[chr]
  fitted_timings = np.array([ff(c) for c in complexities])
  plt.semilogy(complexities, timings, 'ko')
  plt.semilogy(complexities, fitted_timings, 'b-')
  plt.savefig("chr%d.png" % chr)
