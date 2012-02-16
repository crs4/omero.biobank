import sys, argparse, logging

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.kb import KBError


LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']


STUDY_LABEL = {'default'    : 'IMMUNOCHIP',
               'duplicated' : 'IMMUNOCHIP_DUPLICATI'}


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
    parser.add_argument('--source_enroll', type=str, required = True,
                        help='enrollment code of the individual that will be used as source for the merge procedure')
    parser.add_argument('--dest_enroll', type=str, required = True,
                        help='enrollment code of the individual that will be used as destination for the merge procedure')
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

    logger.debug('Retrieving enrollments for study %s' % STUDY_LABEL['default'])
    immunochip = kb.get_enrolled(kb.get_study(STUDY_LABEL['default']))
    logger.debug('Retrieved %d enrollments' % len(immunochip))
    imm_lookup = {}
    for i in immunochip:
        imm_lookup[i.studyCode] = i

    try:
        source = imm_lookup[args.source_enroll]
        logger.info('Selected as source individual with ID %s' % source.individual.id)
        dest = imm_lookup[args.dest_enroll]
        logger.info('Selected as destination individual with ID %s' % dest.individual.id)
    except KeyError, ke:
        logger.error('Code %s not found in study %s' % (ke, STUDY_LABEL['default']))
        sys.exit(2)

    assert source.individual != dest.individual

    logger.debug('Retrieving ActionOnIndividual objects related to source individual')
    query = 'SELECT act FROM ActionOnIndividual act JOIN act.target ind WHERE ind.vid = :ind_vid'
    src_acts = kb.find_all_by_query(query, {'ind_vid' : source.individual.id})
    logger.info('Retrieved %d actions for source individual' % len(src_acts))

    for sa in src_acts:
        sa.target = dest.individual
        kb.save(sa)
        logger.info('Changed target for action %s' % sa.id)

    logger.debug('Retrieving enrollments related to source individual (except for the %s one)' % STUDY_LABEL['default'])
    query = 'SELECT en FROM Enrollment en JOIN en.individual ind JOIN en.study st WHERE ind.vid = :ind_vid AND st.label != :st_label'
    src_enrolls = kb.find_all_by_query(query, {'ind_vid' : source.individual.id, 'st_label' : STUDY_LABEL['default']})
    logger.info('Retrieved %d enrollments related to source individual' % len(src_enrolls))

    for sren in src_enrolls:
        try:
            sren.individual = dest.individual
            kb.save(sren)
            logger.info('Changed individual for enrollment %s (study code %s -- study %s)' % (sren.id,
                                                                                              sren.studyCode,
                                                                                              sren.study.label))
        except KBError, kb:
            logger.warning('Unable to update enrollment %s (study code %s -- study %s)' % (sren.id,
                                                                                           sren.studyCode,
                                                                                           sren.study.label))
            logger.warning(kb)

    logger.info('Changing informations of source enrollment (individual and study)')
    old_ind = source.individual
    source.individual = dest.individual
    source.study = kb.get_study(STUDY_LABEL['duplicated'])
    try:
        source = kb.save(source)
        logger.info('Enrollmnent %s (study code %s -- study %s) now points to individual %s' % (source.id,
                                                                                                source.studyCode,
                                                                                                STUDY_LABEL['default'],
                                                                                                source.individual.id))
    except KBError, kb:
        logger.critical('Unable to update source enrollment')
        logger.critical(kb)

    try:
        kb.delete(old_ind)
        logger.info('Individual %s deleted' % old_ind.id)
    except KBError, kb:
        logger.error('Unable to delete individual %s' % old_ind.id)
        logger.error(kb)

if __name__ == '__main__':
    main(sys.argv[1:])
