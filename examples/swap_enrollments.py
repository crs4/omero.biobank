'''
This script swaps Individual between two Enrollment objects related to
a specific Study.  Input file must be a two columns tsv file without
header like this

1234.1     1234.2
35124.1    35124.3
.....

All codes must be referred to the specific Study passed as input
value.

'''
import sys, argparse, logging, csv

from bl.vl.kb import KnowledgeBase as KB
import bl.vl.utils.ome_utils as vlu

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

def make_parser():
    parser = argparse.ArgumentParser(description = 'Swaps individuals related to two Immunochip enrollment codes',
                                   formatter_class = argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--logfile', type = str, help = 'log file (default = stderr)')
    parser.add_argument('--loglevel', type = str, choices = LOG_LEVELS,
                        help = 'logging level', default = 'INFO')
    parser.add_argument('-H', '--host', type = str, help = 'omero hostname',
                        default = 'localhost')
    parser.add_argument('-U', '--user', type = str, help = 'omero user',
                        default = 'root')
    parser.add_argument('-P', '--passwd', type = str, required = True,
                        help = 'omero password')
    parser.add_argument('-S', '--study', type = str, required = True,
                        help = 'study used for codes lookup')
    parser.add_argument('--couples_list', type = str, required = True,
                        help = 'file containing the codes that will be used for the swap')
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

    with open(args.couples_list) as f:
        reader = csv.reader(f, delimiter='\t')
        couples = []
        for row in reader:
            couples.append((row[0], row[1]))
    logger.info('%d couples are going to be swapped' % len(couples))

    logger.debug('Retrieving enrollments for study %s' % args.study)
    enrolls = kb.get_enrolled(kb.get_study(args.study))
    logger.debug('Retrieved %d enrollments' % len(enrolls))

    en_lookup = {}
    for en in enrolls:
        en_lookup[en.studyCode] = en

    for en_code1, en_code2 in couples:
        logger.info('Swapping couple %s - %s' % (en_code1, en_code2))
        try:
            en1 = en_lookup[en_code1]
            en2 = en_lookup[en_code2]
        except KeyError, ke:
            logger.error('Code %s not found in study %s' % (ke, args.study))
            sys.exit(2)
            
        logger.info('Starting swap procedure for couple %s -- %s' % (en_code1, en_code2))
        logger.debug('Enrollment %s --- Individual ID %s' % (en1.studyCode,
                                                           en1.individual.id))
        logger.debug('Enrollment %s --- Individual ID %s' % (en2.studyCode,
                                                           en2.individual.id))

        en1.individual, en2.individual = en2.individual, en1.individual
        kb.save_array([en1, en2])

        logger.info('Swap completed')
        kb.reload_object(en1)
        logger.debug('Enrollment %s --- Individual ID %s' % (en1.studyCode,
                                                             en1.individual.id))
        kb.reload_object(en2)
        logger.debug('Enrollment %s --- Individual ID %s' % (en2.studyCode,
                                                             en2.individual.id))

        
if __name__ == '__main__':
    main(sys.argv[1:])
