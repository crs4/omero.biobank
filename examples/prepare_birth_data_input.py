# This script takes as input the file produced by get_birth_data.py
# and prepares the input file for the importer birth_data tool

import csv, sys, argparse, logging, time

from bl.vl.kb import KnowledgeBase as KB
import bl.vl.utils.ome_utils as vlu

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

def make_parser():
    parser = argparse.ArgumentParser(description='prepares importer birth_data input file using the get_birth_data.py output')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help='omero password')
    parser.add_argument('--in_file', type=str, required=True,
                        help='input file')
    parser.add_argument('--out_file', type=str, required=True,
                        help='output file')
    return parser

def get_enrollments_lookup(kb, logger):
    logger.info('Loading Enrollments')
    enrolls = kb.get_objects(kb.Enrollment)
    lookup_tb = {}
    for en in enrolls:
        lookup_tb['%s:%s' % (en.study.label, en.studyCode)] = en
    logger.info('Loaded %d Enrollments' % len(lookup_tb))
    return lookup_tb

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

    try:
        host = args.host or vlu.ome_host()
        user = args.user or vlu.ome_user()
        passwd = args.passwd or vlu.ome_passwd()
    except ValueError, ve:
        logger.critical(ve)
        sys.exit(ve)

    kb = KB(driver='omero')(host, user, passwd)
    
    enrolls_map = get_enrollments_lookup(kb, logger)

    logger.info('Loading Individuals')
    inds = kb.get_objects(kb.Individual)
    logger.info('Loaded %d Individuals' % len(inds))

    with open(args.in_file) as in_file, open(args.out_file, 'w') as out_file:
        ts = long(time.time())
        reader = csv.DictReader(in_file, delimiter='\t')
        writer = csv.DictWriter(out_file, ['study', 'individual', 'timestamp',
                                           'birth_date', 'birth_place',
                                           'birth_place_district'],
                                delimiter='\t')
        writer.writeheader()
        for row in reader:
            try:
                writer.writerow({'study' : row['individual'].split(':')[0],
                                 'individual' : enrolls_map[row['individual']].individual.id,
                                 'timestamp' : ts,
                                 'birth_date' : row['birth_date'],
                                 'birth_place' : row['birth_place_code'],
                                 'birth_place_district' : row['birth_place_district']
                                 })
            except KeyError, ke:
                logger.warning('Unable to map %s' % row['individual'])
    

if __name__ == '__main__':
    main(sys.argv[1:])
