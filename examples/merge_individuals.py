import sys, argparse, logging

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.kb import KBError


LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']


def make_parser():
    parser = argparse.ArgumentParser(description='merge informations related to an immunochip enrolled individual to another one')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices = LOG_LEVELS,
                        help='logging level (default=INFO)', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname',
                        default='localhost')
    parser.add_argument('-U', '--user', type=str, help='omero user',
                        default='root')
    parser.add_argument('-P', '--passwd', type=str, required = True,
                        help='omero password')
    parser.add_argument('--source_vid', type=str, required = True,
                        help='VID of the individual that will be used as source for the merge procedure')
    parser.add_argument('--dest_vid', type=str, required = True,
                        help='VID of the individual that will be used as destination for the merge procedure')
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

    kb = KB(driver='omero')(args.host, args.user, args.passwd)

    logger.debug('Retrieving Individuals')
    individuals = kb.get_objects(kb.Individual)
    logger.debug('Retrieved %d Individuals' % len(individuals))
    ind_lookup = {}
    for i in individuals:
        ind_lookup[i.id] = i

    try:
        source = ind_lookup[args.source_vid]
        logger.info('Selected as source individual with ID %s' % source.id)
        dest = ind_lookup[args.dest_vid]
        logger.info('Selected as destination individual with ID %s' % dest.id)
    except KeyError, ke:
        logger.error('Unable to retrieve individual with ID %s' % ke)
        sys.exit(2)

    logger.debug('Retrieving ActionOnIndividual objects related to source individual')
    query = 'SELECT act FROM ActionOnIndividual act JOIN act.target ind WHERE ind.vid = :ind_vid'
    src_acts = kb.find_all_by_query(query, {'ind_vid' : source.id})
    logger.info('Retrieved %d actions for source individual' % len(src_acts))

    for sa in src_acts:
        sa.target = dest
        kb.save(sa)
        logger.info('Changed target for action %s' % sa.id)

    logger.debug('Retrieving enrollments related to source individual')
    query = 'SELECT en FROM Enrollment en JOIN en.individual ind WHERE ind.vid = :ind_vid'
    src_enrolls = kb.find_all_by_query(query, {'ind_vid' : source.id})
    logger.info('Retrieved %d enrollments related to source individual' % len(src_enrolls))

    for sren in src_enrolls:
        try:
            sren.individual = dest
            kb.save(sren)
            logger.info('Changed individual for enrollment %s (study code %s -- study %s)' % (sren.id,
                                                                                              sren.studyCode,
                                                                                              sren.study.label))
        except KBError, kbe:
            logger.warning('Unable to update enrollment %s (study code %s -- study %s)' % (sren.id,
                                                                                           sren.studyCode,
                                                                                           sren.study.label))
            logger.warning(kbe)

    logger.info('Moving EHR records from source to destination')
    kb.update_table_rows(kb.eadpt.EAV_EHR_TABLE, '(i_vid == "%s")' % source.id, {'i_vid' : dest.id})
    logger.info('EHR records updated')
    
    try:
        kb.delete(source)
        logger.info('Individual %s deleted' % source.id)
    except KBError, kb:
        logger.error('Unable to delete individual %s' % source.id)
        logger.error(kb)

if __name__ == '__main__':
    main(sys.argv[1:])
