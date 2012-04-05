'''
From a file like this
individual                           sibling
V08E18411BC66F4987BCA43EFC6F636224   V0AE5660BF4A7149589BE9DB3308B50327
V0FAE2B10F690041509739A3F4B314DC8F   V00875417B31684EC2A62EE37717913445
V0382EF862AA4B475697C95D3777043239   V08E376727ED8E4B369DAA3B62A9395E1B
....

retrieve indivual's parents using sibling informations and build a file like

individual                           father                               mother
V08E18411BC66F4987BCA43EFC6F636224   V027DE334753424F07B81A70053EF5B873   V035222CAEE0474AFEBB9A161D4B64914E
V0FAE2B10F690041509739A3F4B314DC8F   V0E966B53BDCC942C09D6B6D96DE98F4F4   V0F7B6926C6FBE4F0BB38BBC6CFB13A825
....

'''

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
    parser = argparse.ArgumentParser(description='retrieve parents information using sibling')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level (default=INFO)', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, required=True)
    parser.add_argument('--in_file', type=str, required=True,
                        help='input file with individual-sibling couples')
    parser.add_argument('--out_file', type=str, required=True,
                        help='output file with parents information')
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
        host = args.host if args.host else ome_host()
        user = args.user if args.user else ome_user()
        passwd = args.passwd if args.passwd else ome_passwd()
    except ValueError, ve:
        logger.critical(ve)
        sys.exit(ve)

    kb = KB(driver='omero')(host, user, passwd)

    logger.info('Retrieving individuals')
    inds = kb.get_objects(kb.Individual)
    logger.info('Retrieved %d individuals' % len(inds))
    inds_lookup = {}
    for ind in inds:
        inds_lookup[ind.id] = ind

    with open(args.in_file) as infile:
        reader = csv.DictReader(infile, delimiter='\t')
        records = []
        for row in reader:
            try:
                sib = inds_lookup[row['sibling']]
                rec = {'individual' : row['individual'],
                       'father'     : sib.father.id if sib.father else 'None',
                       'mother'     : sib.mother.id if sib.mother else 'None'}
                logger.info('Individual %s, father: %s - mother: %s' % (row['individual'],
                                                                        rec['father'],
                                                                        rec['mother']))
                records.append(rec)
            except KeyError:
                logger.error('Unable to find individual %s' % row['sibling'])

    logger.info('Retrieved parents for %d individuals' % len(records))

    with open(args.out_file, 'w') as outfile:
        writer = csv.DictWriter(outfile, ['individual', 'father', 'mother'],
                                delimiter='\t')
        writer.writeheader()
        writer.writerows(records)

if __name__ == '__main__':
    main(sys.argv[1:])
