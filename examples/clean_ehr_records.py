import sys, argparse, logging

import bl.vl.utils.ome_utils as vlu
from bl.vl.kb import KnowledgeBase as KB

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

def make_parser():
    parser = argparse.ArgumentParser(description = 'This tool will find identical EHR records for all the individuals in the system and will invalidate all the duplicated')
    parser.add_argument('--logfile', type=str, help='log file (default = stderr)')
    parser.add_argument('--loglevel', type=str, choices = LOG_LEVELS,
                        help = 'logging level', default = 'INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help = 'omero password')
    return parser

def get_ehr_key(ehr_record):
    return repr([ehr_record['i_id'], ehr_record['valid'], ehr_record['archetype'],
                 ehr_record['fields']])

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

    logging.info('Loading EHR records')
    ehr_records = kb.get_ehr_records()
    logging.info('Loaded %d EHR records' % len(ehr_records))

    ehr_lookup = {}
    for rec in ehr_records:
        key = get_ehr_key(rec)
        ehr_lookup.setdefault(rec['i_id'], {}).setdefault(key,[]).append(rec)
    logger.debug('Loaded EHR lookup table, %d individuals involved' %
                 len(ehr_lookup.keys()))

    for ind, records in ehr_lookup.iteritems():
        for k, vals in records.iteritems():
            if len(vals) > 1:
                logger.info('##### Individual %s has %d duplicated EHR records #####' % (ind,
                                                                                         len(vals)))
                logger.debug(vals)
                for v in vals[1:]:
                    logger.info('Invalidating record %r' % v)
                    kb.invalidate_ehr_records(v['i_id'], timestamp = v['timestamp'],
                                              archetype = v['archetype'],
                                              grouper_id = v['g_id'],
                                              field = v['fields'].keys()[0],
                                              field_value = str(v['fields'].values()[0]))

if __name__ == '__main__':
    main(sys.argv[1:])
