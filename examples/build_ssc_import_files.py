"""

Read all SSC files from a directory and build input files that will be
used to import DataSample and DataObject items into Omero using a
mapping file to retrieve the proper source for each SSC file.

TSV input file for DataSample will be like this::

  label      source     device                       device_type      data_sample_type    markers_set           status
  V1234.ssc  PL12::W01  Illumina-GenomeStudio-1.8.4  SoftwareProgram  GenotypeDataSample  IMMUNO_BC_11419691_B  USABLE
  .......

TSV input file for DataObject will be like this::

  path                data_sample_label   mimetype        size    sha1
  /FOODIR/V1234.ssc   V1234.ssc           x-ssc-messages  20723   7a95eae3c0ed46b377a80f23eed8d5016ce7ed6f

TSV mapping file must be like::

  ssc_label    source_label
  V1234.ssc    PL12::W01
  ...

"""
import sys, os, argparse, csv, logging

from bl.vl.utils import compute_sha1
from bl.vl.kb import mimetypes

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

DS_FN = 'import_data_sample.tsv'
DO_FN = 'import_data_object.tsv'

DEVICE_TYPE = 'SoftwareProgram'
DATA_SAMPLE_TYPE = 'GenotypeDataSample'

class Writer(csv.DictWriter):
    def __init__(self, f, fieldnames):
        csv.DictWriter.__init__(self, f, fieldnames, delimiter='\t',
                                lineterminator='\n')

class DataSampleWriter(Writer):
    def __init__(self, f):
        Writer.__init__(self, f, ['label', 'source', 'device', 'device_type',
                                  'data_sample_type', 'markers_set', 'status'])

class DataObjectWriter(Writer):
    def __init__(self, f):
        Writer.__init__(self, f, ['path', 'data_sample_label', 'mimetype',
                                  'size', 'sha1'])


def make_parser():
    parser = argparse.ArgumentParser(description='write SSC import files')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('--ssc-dir', type=str, required=True,
                        help='directory containing SSC files that will be parsed')
    parser.add_argument('--map-file', type=str, required=True,
                        help='mapping file with source-SSC association')
    parser.add_argument('--ds-fn', metavar='DS_FILE', default=DS_FN,
                        help='output path for import data sample tsv file')
    parser.add_argument('--do-fn', metavar='DO_FILE', default=DO_FN,
                        help='output path for import data object tsv file')
    parser.add_argument('--device-label', type=str, help='device label',
                        required=True)
    parser.add_argument('--marker-set-label', type=str, help='marker set label',
                        required=True)
    return parser


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.loglevel)
    kwargs = {'format': LOG_FORMAT,
              'datefmt': LOG_DATEFMT,
              'level': log_level}
    logging.basicConfig(**kwargs)
    logger = logging.getLogger()

    # Get all sources mapped into a dict
    with open(args.map_file) as mapfile:
        reader = csv.DictReader(mapfile, delimiter='\t')
        ssc_src_map = {}
        for row in reader:
            ssc_src_map[row['ssc_label']] = row['source_label']

    ssc_files = os.listdir(args.ssc_dir)
    for ssc in ssc_files:
        if not ssc.endswith('.ssc'):
            logger.debug('%s is not a SSC file, this element will be ignored' % ssc)
            ssc_files.remove(ssc)
    logger.info('%d files ready to be processed' % len(ssc_files))

    with open(args.ds_fn, 'w') as ds_f, open(args.do_fn, 'w') as do_f:
        ds_w = DataSampleWriter(ds_f)
        do_w = DataObjectWriter(do_f)
        for w in ds_w, do_w:
            w.writeheader()
        for ssc_f in ssc_files:
            logger.info('Processing %s (file %d/%d)' % (ssc_f, ssc_files.index(ssc_f)+1,
                                                        len(ssc_files)))
            try:
                with open(os.path.join(args.ssc_dir, ssc_f)) as infile:
                    ds_w.writerow({
                            'label': os.path.basename(infile.name),
                            'source': ssc_src_map[os.path.basename(infile.name)],
                            'device': args.device_label,
                            'device_type': DEVICE_TYPE,
                            'data_sample_type': DATA_SAMPLE_TYPE,
                            'markers_set': args.marker_set_label,
                            'status': 'USABLE',
                            })
                    do_w.writerow({
                            'path': os.path.realpath(infile.name),
                            'data_sample_label': os.path.basename(infile.name),
                            'mimetype': mimetypes.SSC_FILE,
                            'size' : str(os.stat(infile.name).st_size),
                            'sha1': compute_sha1(infile.name)
                            })
            except KeyError, ke:
                logger.error('File %s has no source mapped, skipping line' % ke)

if __name__ == '__main__':
    main(sys.argv[1:])
