# This tool fetches OMNIEXPRESS related wells and matches them to
# connected individuals in order to build a file that can be used with
# the enrollments importer tool in order to import new OMNIEXPRESS
# enrollments into the system.

import csv, sys, argparse, logging

from bl.vl.kb import KnowledgeBase as KB
import bl.vl.kb.drivers.omero.utils as vlu

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

OE_STCODE_PATTERN = 'OE_%010d'
OE_LABEL_PREFIX = 'OE_'

def make_parser():
    parser = argparse.ArgumentParser(description='retrieves individuals with an OMNIEXPRESS related well and build file needed to import new enrollments inside OMERO')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices = LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help='omero password')
    parser.add_argument('--ofile', type=str, required=True,
                        help='output file')
    return parser

def get_first_index(kb, logger):
    logger.debug('Loading enrollments from OMNIEXPRESS study')
    oexpress = kb.get_enrolled(kb.get_study('OMNIEXPRESS'))
    logger.debug('Found %d enrolled individuals' % len(oexpress))
    if len(oexpress) > 0:
        first_index = max([int(oe.studyCode.replace('OE_', '')) for oe in oexpress]) + 1
    else:
        first_index = 1
    logger.info('Using %d as first index for OMNIEXPRESS enrollment codes' % first_index)
    return first_index

def get_omniexpress_plates(kb):
    return [pl for pl in kb.get_objects(kb.TiterPlate) \
                if pl.label.startswith(OE_LABEL_PREFIX)]

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
        host   = args.host or vlu.ome_host()
        user   = args.user or vlu.ome_user()
        passwd = args.passwd or vlu.ome_passwd()
    except ValueError, ve:
        logger.critical(ve)
        sys.exit(ve)

    kb = KB(driver='omero')(host, user, passwd)

    plates = get_omniexpress_plates(kb)
    logger.info('Retrieved %d OMNIEXPRESS related plates' % len(plates))

    logger.info('Calculating dependency tree')
    kb.update_dependency_tree()
    logger.info('Dependency tree loaded')

    with open(args.ofile, 'w') as ofile:
        writer = csv.DictWriter(ofile, ['source', 'study', 'label'],
                                delimiter = '\t')
        writer.writeheader()

        oe_index = get_first_index(kb, logger)
        for pl in plates:
            wells = list(kb.get_wells_by_plate(pl))
            logger.info('Loaded %d wells for plate %s' % (len(wells),
                                                          pl.barcode))
            for w in wells:
                ind = kb.dt.get_connected(w, kb.Individual)[0]
                writer.writerow({'source' : ind.id,
                                 'study'  : 'OMNIEXPRESS',
                                 'label'  : OE_STCODE_PATTERN % oe_index})
                oe_index += 1
    logger.info('Job completed')

if __name__ == '__main__':
    main(sys.argv[1:])

