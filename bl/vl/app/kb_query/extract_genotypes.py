# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Extract genotype data in tabular format, e.g.::

  AA AB NN BB AB
  BB NN AA AA AB
  AA AA BB BB AB

where colums represent data samples and rows represent SNPs.

**NOTE:** if the output is too big to fit into memory, activate the
``--transpose_output`` switch.  Output data will be transposed, but
the script will only require enough memory to store a single sample.
"""

import os, argparse, csv, bz2, time
import numpy as np
from collections import Counter

from bl.vl.app.importer.core import Core
from bl.vl.genotype.algo import project_to_discrete_genotype


class Writer(object):

    def __init__(self, genotypes_out_file, samples_list_out_file,
                 transpose_output=False, ignore_duplicated=False,
                 logger = None):
        self.out_gt_file = genotypes_out_file
        self.out_ds_file = samples_list_out_file
        self.out_gt_csvw = csv.writer(self.out_gt_file, delimiter='\t')
        self.out_ds_csvw = csv.writer(self.out_ds_file, delimiter='\t')
        self.tro = transpose_output
        self.igd = ignore_duplicated
        self.out_data = []
        self.logger = logger
        self.counter = Counter()

    def write_record(self, individual, data_samples,
                     data_collection_samples=None):
        allele_patterns = {0: 'AA', 1: 'BB', 2:'AB', 3: 'NN'}
        if data_collection_samples:
            dsamples = [d for d in data_samples if d in data_collection_samples]
        else:
            dsamples = data_samples
        if len(dsamples) > 0:
            if self.igd:
                dsamples = dsamples[:1]
            for ds in dsamples:
                start = time.time()
                probs, _ = ds.resolve_to_data()
                end = time.time() - start
                self.counter['total_fetch_time'] += end
                if end < self.counter['faster_fetch'] or 'faster_fetch' not in self.counter:
                    self.counter['faster_fetch'] = end
                if end > self.counter['slower_fetch']:
                    self.counter['slower_fetch'] = end
                self.logger.debug('Retrieved data for %s in %f seconds' %
                                  (ds.label, end))
                if probs is not None:
                    self.counter['fetched_samples'] += 1
                    self.out_ds_csvw.writerow([ds.id])
                    disc_probs = [allele_patterns[x]
                                  for x in project_to_discrete_genotype(probs)]
                    if self.tro:
                        self.out_gt_csvw.writerow(disc_probs)
                    else:
                        self.out_data.append(disc_probs)

    def close(self):
        if len(self.out_data) > 0:
            self.out_data = np.array(self.out_data).transpose()
            for d in self.out_data:
                self.out_gt_csvw.writerow(d)
        self.out_gt_file.close()
        self.out_ds_file.close()
        self.logger.debug('########## Samples fetching statistics ##########')
        self.logger.debug('%d samples fetched in %f seconds' % (self.counter['fetched_samples'],
                                                                 self.counter['total_fetch_time']))
        self.logger.debug('Average sample fetch time: %f seconds' % 
                          (self.counter['total_fetch_time'] / self.counter['fetched_samples']))
        self.logger.debug('Faster fetch: %f seconds' % self.counter['faster_fetch'])
        self.logger.debug('Slower fetch: %f seconds' % self.counter['slower_fetch'])
        self.logger.debug('#################################################')


class App(Core):

    def __init__(self, host=None, user=None, passwd=None, keep_tokens=1,
                 logger=None, study_label=None, operator='Alfred E. Neumann'):
        super(App, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                  study_label=study_label, logger=logger)

    def get_data_samples_map(self, individuals, markers_set):
        ds_map = {}
        for i in individuals:
            ds_map[i] = list(self.kb.get_genotype_data_samples(i, markers_set))
        return ds_map

    def dump(self, genotypes_out_file, samples_list_out_file, marker_set_label,
             data_collection_label=None, transpose_output=False,
             ignore_duplicated=False, enable_compression=False,
             compression_level=None):
        self.logger.info(
            'Loading individuals from study %s' % self.default_study.label
            )
        inds = [en.individual
                for en in self.kb.get_enrolled(self.default_study)]
        self.logger.info('Loaded %d individuals' % len(inds))
        self.logger.info('Loading marker set %s' % marker_set_label)
        mset = self.kb.get_snp_markers_set(marker_set_label)
        if data_collection_label:
            self.logger.info('Loading elements from data collection %s' %
                             data_collection_label)
            dcoll = self.kb.get_data_collection(data_collection_label)
            dc_samples = [dci.dataSample
                          for dci in self.kb.get_data_collection_items(dcoll)]
            self.logger.info('Loaded %d elements' % len(dc_samples))
        else:
            dc_samples = None
        data_samples_map = self.get_data_samples_map(inds, mset)
        self.logger.info('Initializing writer')
        if enable_compression:
            genotypes_out_file.close()
            genotypes_out_file = bz2.BZ2File(
                os.path.abspath(genotypes_out_file.name),
                'w', compression_level
                )
        kw_args = {
            'transpose_output': transpose_output,
            'ignore_duplicated': ignore_duplicated,
            'genotypes_out_file': genotypes_out_file,
            'samples_list_out_file': samples_list_out_file,
            'logger' : self.logger
            }
        writer = Writer(**kw_args)
        self.logger.info('Writing records')
        for ind, dsamples in data_samples_map.iteritems():
            self.logger.debug(
                'Writing record for individual %s (%d/%d)' % (
                    ind.id,
                    data_samples_map.keys().index(ind) + 1, 
                    len(inds)
                    ))
            writer.write_record(ind, dsamples, dc_samples)
        self.logger.info('Closing writer')
        writer.close()
        self.logger.info('Job complete')


def make_parser(parser):
    parser.add_argument('--out_samples_list', type=argparse.FileType('w'),
                        help='output files with samples VID', required=True)
    parser.add_argument('--study', type=str, help='study label', required=True)
    parser.add_argument('--marker_set', type=str, help='marker set label',
                        required=True)
    parser.add_argument('--data_collection', type=str,
                        help='data collection label', default=None)
    parser.add_argument('--transpose_output', action='store_true',
                        help='transpose output data (rows = samples)')
    parser.add_argument('--ignore_duplicated', action='store_true',
                        help='use only the 1st data sample for each individual')
    parser.add_argument('--compress_output', action='store_true',
                        help='write output files in bzip2-compressed format')
    parser.add_argument('--compression_level', type=int, choices=range(1, 10),
                        help='compression level (1 to 9)', default=5)


def implementation(logger, host, user, passwd, args):
    app = App(host=host, user=user, passwd=passwd,
              keep_tokens=args.keep_tokens, logger=logger,
              study_label=args.study)
    app.dump(args.ofile, args.out_samples_list, args.marker_set,
             args.data_collection, args.transpose_output,
             args.ignore_duplicated, args.compress_output,
             args.compression_level)


def do_register(registration_list):
    help_doc = "Extract genotype data in tabular format"
    registration_list.append(('extract_gt', help_doc, make_parser,
                              implementation))
