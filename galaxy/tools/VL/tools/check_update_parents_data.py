import sys, csv, argparse, logging, os

from bl.vl.kb import KnowledgeBase as KB

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

def ome_env_variable(name):
    if os.environ.has_key(name):
        return os.environ[name]
    else:
        msg = 'Can\'t use default parameter, environment variable %s does not exist' % name
        raise ValueError(msg)

def ome_host():
    return ome_env_variable('OME_HOST')

def ome_user():
    return ome_env_variable('OME_USER')

def ome_passwd():
    return ome_env_variable('OME_PASSWD')

def make_parser():
    parser = argparse.ArgumentParser(description='check data that will be passed to the update_parents tool')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level (default=INFO)', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help='omero password')
    parser.add_argument('--in_file', type=str, required=True,
                        help='input file')
    parser.add_argument('--out_file', type=str, required=True,
                        help='output file')
    return parser

def check_row(row, individuals_map, kb):
    logger = logging.getLogger()
    logger.debug('Checking record %r' % row)
    try:
        ind = individuals_map[row['individual']]
        logger.info('%s is a valid Individual ID' % ind.id)
        if row['father'] != 'None':
            father = individuals_map[row['father']]
            logger.info('%s is a valid Individual ID' % father.id)
            check_gender(father, kb.Gender.MALE)
            logger.info('Gender check passed')
        else:
            logger.info('None value, no check required')
        if row['mother'] != 'None':
            mother = individuals_map[row['mother']]
            logger.info('%s is a valid Individual ID' % mother.id)
            check_gender(mother, kb.Gender.FEMALE)
            logger.info('Gender check passed')
        else:
            logger.info('None value, no check required')
        return True
    except KeyError, ke:
        logger.error('%s is not a valid Individual ID, rejecting row' % ke)
        return False
    except ValueError, ve:
        logger.error(ve)
        return False

def check_gender(individual, gender):
    if individual.gender.enum_label() != gender.enum_label():
        raise ValueError('Gender for individual %s is %s, expected %s, rejecting row' % (individual.id,
                                                                                         individual.gender.enum_label(),
                                                                                         gender.enum_label()))
    else:
        pass

def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.loglevel)
    kwargs = {'format': LOG_FORMAT,
              'datefmt': LOG_DATEFMT,
              'level': log_level}
    if args.logfile:
        kwargs['filename'] = args.logfile
    logging.basicConfig(**kwargs)
    logger = logging.getLogger()

    try:
        host = args.host if args.hots else ome_host()
        user = args.user if args.user else ome_user()
        passwd = args.passwd if args.passwd else ome_passwd()
    except ValueError, ve:
        logger.error(ve)
        sys.exit(2)

    kb = KB(driver='omero')(host, user, passwd)

    logger.info('Preloading all individuals from the system')
    inds = kb.get_objects(kb.Individual)
    logger.info('%d individuals loaded' % len(inds))
    inds_lookup = {}
    for i in inds:
        inds_lookup[i.id] = i

    with open(args.in_file) as infile, open(args.out_file, 'w') as outfile:
        reader = csv.DictReader(infile, delimiter='\t')
        writer = csv.DictWriter(outfile, reader.fieldnames, delimiter='\t')
        writer.writeheader()
        for row in reader:
            if check_row(row, inds_lookup, kb):
                writer.writerow(row)
                logger.debug('Record %r written in output file' % row)

if __name__ == '__main__':
    main(sys.argv[1:])
