"""
This script generates the input file for the hadoop version of the
kinship algorithm.
The output file will be like

AA AB NN BB AB
BB NN AA AA AB
AA AA BB BB AB
...

* each colum represents a single data sample
* each row represents a specific SNP

NOTE WELL: if the output file is too big to be handled with the RAM of
the computer that runs this script, the file can be written as the
transposed version of the output format described above (each row will
represent a data sample and each column a SNP) and transposed using a
specific map-reduce job.

"""

import sys, os, argparse, csv, logging
import numpy as np

from bl.vl.kb import KnowledgeBase as KB
import bl.vl.kb.drivers.omero.utils as vlu
from bl.vl.genotype.algo import project_to_discrete_genotype


LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']


class KinshipWriter(object):
    """
    TODO: write something....
    """
    def __init__(self, mset, path, base_filename = 'bl-vl-kinship',
                 transpose_output = False, ignore_duplicated = False):
        self.mset = mset
        self.out_k_file = open(os.path.join(path, '%s.genotypes' % base_filename),
                               'w')
        self.out_k_csvw = csv.writer(self.out_k_file, delimiter='\t')
        self.out_ds_file = open(os.path.join(path, '%s.data_samples' % base_filename),
                               'w')
        self.out_ds_csvw = csv.writer(self.out_ds_file, delimiter='\t')
        self.tro = transpose_output
        self.igd = ignore_duplicated
        self.out_data = []
        self.kb = self.mset.proxy

    def write_record(self, individual, data_collection_samples = None):
        """
        TODO: doc here...
        """
        allele_patterns = {0: 'AA', 1: 'BB', 2:'AB', 3: 'NN'}
        dsamples = self.kb.get_data_samples(individual, 'GenotypeDataSample')
        # First of all, filter by marker set
        dsamples = [d for d in dsamples if d.snpMarkersSet == self.mset]
        # If data_collection_items list has been provided, keep only
        # data samples that belongs to this list
        if data_collection_samples:
            dsamples = [d for d in dsamples if d in data_collection_samples]
        if len(dsamples) > 0:
            if self.igd:
                dsamples = dsamples[:1]
            for ds in dsamples:
                probs, _ = ds.resolve_to_data()
                if probs is not None:
                    self.out_ds_csvw.writerow([ds.id])
                    disc_probs = [allele_patterns[x]
                                  for x in project_to_discrete_genotype(probs)]
                    if self.tro:
                        self.out_k_csvw.writerow(disc_probs)
                    else:
                        self.out_data.append(disc_probs)
        
    def close(self):
        """
        TODO: doc here
        """
        if len(self.out_data) > 0:
            self.out_data = np.array(self.out_data).transpose()
            for d in self.out_data:
                self.out_k_csvw.writerow(d)
        self.out_k_file.close()
        self.out_ds_file.close()
        

def make_parser():
    parser = argparse.ArgumentParser(description = 'build kinship input from VL')
    parser.add_argument('--logfile', type = str, help = 'log file(default=stderr)')
    parser.add_argument('--loglevel', type = str, choices = LOG_LEVELS,
                        help = 'logging level', default = 'INFO')
    parser.add_argument('--study', type = str, help = 'study label',
                        required = True)
    parser.add_argument('--marker_set', type = str, help = 'marker set label',
                        required = True)
    parser.add_argument('--data_collection', type = str, help = 'data collection label',
                        default = None)
    parser.add_argument('--transpose_output', action='store_true',
                        help = 'write the transposed version of the standard output')
    parser.add_argument('--ignore_duplicated', action='store_true',
                        help = 'if more than one data sample is connected to an indiviudal use only the first one')
    parser.add_argument('--host', '-H', type = str,
                        help = 'omero hostname (default ${OME_HOST})')
    parser.add_argument('--user', '-U', type = str,
                        help = 'omero user (default ${OME_USER})')
    parser.add_argument('--passwd', '-P', type = str,
                        help  = 'omero password (default ${OME_PASSWD})')
    parser.add_argument('--out_path', type = str, help = 'output files path',
                        required = True)
    parser.add_argument('--base_filename', type = str, default = None,
                        help = 'base name for output files')
    return parser


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.loglevel)
    kwargs = {'format' : LOG_FORMAT,
              'datefmt' : LOG_DATEFMT,
              'level' : log_level}
    if args.logfile:
        kwargs['filename'] = args.logfile
    logging.basicConfig(**kwargs)
    logger = logging.getLogger()

    try:
        host = args.host or vlu.ome_host()
        user = args.user or vlu.ome_user()
        passwd = args.passwd or vlu.ome_passwd()
    except ValueError, ve:
        logger.critical(ve)
        sys.exit(ve)

    kb = KB(driver='omero')(host, user, passwd)
    
    logger.info('Loading individuals from study %s' % args.study)
    inds = [en.individual
            for en in kb.get_enrolled(kb.get_study(args.study))]
    logger.info('Loaded %d individuals' % len(inds))

    logger.info('Loading marker set %s' % args.marker_set)
    mset = kb.get_snp_markers_set(args.marker_set)

    if args.data_collection:
        logger.info('Loading elements from data collection %s' % args.data_collection)
        dcoll = kb.get_data_collection(args.data_collection)
        dc_samples = [dci.dataSample 
                      for dci in kb.get_data_collection_items(dcoll)]
        logger.info('Loaded %d elements' % len(dc_samples))
    else:
        dc_samples = None

    logger.info('Initializing writer')

    kw_args = {'mset' : mset,
               'path' : args.out_path,
               'transpose_output' : args.transpose_output,
               'ignore_duplicated' : args.ignore_duplicated}
    if args.base_filename:
        kw_args['base_filename'] = args.base_filename
        
    kinship_writer = KinshipWriter(**kw_args)
    
    logger.info('Writing records')
    for ind in inds:
        logger.debug('Writing record for individual %s (%d/%d)' % (ind.id,
                                                                   inds.index(ind) + 1,
                                                                   len(inds)))
        kinship_writer.write_record(ind, dc_samples)

    logger.info('Closing writer')
    kinship_writer.close()
    logger.info('Job complete')


if __name__ == '__main__':
    main(sys.argv[1:])
