import sys, argparse, logging

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.kb import KBError
import bl.vl.utils.ome_utils as vlu

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

def make_parser():
    parser = argparse.ArgumentParser(description='move an immunochip related enrollment to "discarded" study and mark related wells as unusable',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices = LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help='omero password')
    parser.add_argument('--source_study', type=str, default='IMMUNOCHIP',
                        help='source study for the lookup')
    parser.add_argument('--discard_study', type=str, default='IMMUNOCHIP_DISCARDED',
                        help='study that will be associated to discarded enrollments')
    parser.add_argument('--source_list', type=str, required = True,
                        help='file containing the list of enrollments that will be discarded')
    return parser

def mark_invalid_well(enrollment, kb):
    logger = logging.getLogger()
    wells = kb.get_vessels_by_individual(enrollment.individual, 'PlateWell')
    wells = [w for w in wells]
    logger.info('Retrieved %d well(s) for %s' % (len(wells), enrollment.studyCode))
    for w in wells:
        # Only the well related to the specific studyCode will be
        # marked as invalid, this can be done ONLY using IMMUNOCHIP
        # enrollments!
        pl_barcode = enrollment.studyCode.split('|')[1]
        if w.container.barcode == pl_barcode:
            logger.info('Well %s for plate %s (barcode %s) will be discarded' % (w.label,
                                                                                 w.container.label,
                                                                                 w.container.barcode))
            w.status = kb.VesselStatus.DISCARDED
            return w
        else:
            logger.debug('Well %s of plate %s (barcode %s) ignored' % (w.label, w.container.label,
                                                                       w.container.barcode))
    logger.warning('Nothing has been updated!')
    return None

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

    logger.debug('Reading codes from source list')
    with open(args.source_list) as f:
        codes = [row.strip() for row in f.readlines()]
    logger.debug('Found %d codes to discard' % len(codes))

    logger.debug('Retrieving enrollments for study %s' % args.source_study)
    source_enrolls = kb.get_enrolled(kb.get_study(args.source_study))
    logger.debug('Retrieved %d enrollments' % len(source_enrolls))
    src_st_lookup = {}
    for sen in source_enrolls:
        src_st_lookup[sen.studyCode] = sen

    to_be_discarded = []
    discard_st = kb.get_study(args.discard_study)
    if discard_st is None:
        logger.critical('Study with label %s not found!' % args.discard_study)
        sys.exit(2)

    for c in codes:
        try:
            src_st_lookup[c].study = discard_st
            to_be_discarded.append(src_st_lookup[c])
        except KeyError:
            logger.warning('Enrollment %s not found in study %s' % (c, args.source_study))
    logger.info('%d enrollments will be discarded' % len(to_be_discarded))


    for disc in to_be_discarded:
        try:
            kb.save(disc)
        except KBError:
            logger.error('Can\'t save enrollment %s in study %s' % (disc.studyCode,
                                                                    disc.study.label))
            continue
        disc_well = mark_invalid_well(disc, kb)
        if disc_well:
            kb.save(disc_well)
            pass

if __name__ == '__main__':
    main(sys.argv[1:])
