# This tool format output files from kb_query vessels_by_individual
# into a tabular format with all data related to an individual grouped
# in each row. The tool needs as input a mapping file like
#
#  individual_id   label
#  V12311          A_STUDY:A_CODE
#  V135115         A_STUDY:B_CODE
#
# in order to use a known label and not VIDs for each row

import csv, sys, argparse, logging

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

def make_parser():
    parser = argparse.ArgumentParser(description='format kb_query vessels_by_individual output file to tabular format')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('--in_file', type=str, required=True,
                        help='input file (obtained using kb_query vessels by individual tool)')
    parser.add_argument('--map_file', type=str, required=True,
                        help='mapping file')
    parser.add_argument('--out_file', type=str, required=True,
                        help='output file')
    return parser

def get_mapping(records, grouper_field, grouped_field):
    mapping = {}
    for rec in records:
        mapping.setdefault(rec[grouper_field], []).append(rec[grouped_field])
    return mapping

def get_labels_mapping(reader, logger):
    rows = [r for r in reader]
    lmap = get_mapping(rows, 'individual', 'label')
    logger.info('%d labels grouped for %d individuals' % (len(rows),
                                                          len(lmap)))
    return lmap

def get_vessels_mapping(reader, logger):
    rows = [r for r in reader]
    vmap = get_mapping(rows, 'individual', 'vessel_label')
    logger.info('%d vessels grouped for %d individuals' % (len(rows),
                                                           len(vmap)))
    return vmap

def build_record(label, vessels):
    record = {'individual_label' : '--'.join(label)}
    for v in vessels:
        record['vessel_%d' % (vessels.index(v) + 1)] = v
    return record

def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.loglevel)
    kwargs = {'format'  : LOG_FORMAT,
              'datefmt' : LOG_DATEFMT,
              'level'   : log_level}
    if args.logfile:
        kwargs['filename'] = args.logfile
    logging.basicConfig(**kwargs)
    logger = logging.getLogger()

    with open(args.map_file) as mf:
        reader = csv.DictReader(mf, delimiter='\t')
        labels_map = get_labels_mapping(reader, logger)

    with open(args.in_file) as inf:
        reader = csv.DictReader(inf, delimiter='\t')
        vessels_map = get_vessels_mapping(reader, logger)

    max_vessels_count = max([len(v) for v in vessels_map.values()])
    csv_fields = ['individual_label']
    for x in xrange(max_vessels_count):
        csv_fields.append('vessel_%d' % (x+1))

    with open(args.out_file, 'w') as ofile:
        writer = csv.DictWriter(ofile, csv_fields, delimiter='\t')
        writer.writeheader()
        for ind, vessels in vessels_map.iteritems():
            writer.writerow(build_record(labels_map[ind], vessels))

    logger.info('Job completed')

if __name__ == '__main__':
    main(sys.argv[1:])
