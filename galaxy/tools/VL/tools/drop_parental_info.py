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
    parser = argparse.ArgumentParser(description='set parents of the selected individuals to None')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level (default=INFO)', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help='omero password')
    parser.add_argument('--in_file', type=str, required=True,
                        help='list of the individuals')
    parser.add_argument('--out_file', type=str, required=True,
                        help='output file')
    return parser

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
        host = args.host if args.host else ome_host()
        user = args.user if args.user else ome_user()
        passwd = args.passwd if args.passwd else ome_passwd()
    except ValueError, ve:
        logger.error(ve)
        sys.exit(2)

    kb = KB(driver='omero')(host, user, passwd)

    logger.info('Retrieving individuals')
    inds = kb.get_objects(kb.Individual)
    logger.info('Retrieved %d individuals' % len(inds))
    inds_lookup = {}
    for i in inds:
        inds_lookup[i.id] = i

    with open(args.in_file) as in_file:
        reader = csv.DictReader(in_file, delimiter='\t')
        records = []
        for row in reader:
            try:
                # The 'individual' : inds_lookup[row['individual']].id
                # is quite redundant but is a usefull check in order
                # to filter wrong VIDs
                record = {'individual' : inds_lookup[row['individual']].id,
                          'father' : 'None',
                          'mother' : 'None'}
                records.append(record)
            except KeyError, ke:
                logger.warning('Individual with VID %s does not exist, skipping line' % ke)

    with open(args.out_file, 'w') as out_file:
        writer = csv.DictWriter(out_file, ['individual', 'father', 'mother'],
                                delimiter = '\t')
        writer.writeheader()
        writer.writerows(records)

if __name__ == '__main__':
    main(sys.argv[1:])
