import logging, csv, argparse, sys, os

from bl.vl.kb import KnowledgeBase as KB
import bl.vl.kb.drivers.omero.utils as vlu

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

CSV_FIELDS = ['label', 'barcode', 'rows', 'columns', 'plate_status']

def make_parser():
    parser = argparse.ArgumentParser(description='dump all TiterPlate objects to a TSV file')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help='omero password')
    parser.add_argument('--ofile', type=str, help='output file',
                        required=True)
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
        passwd = args.passwd or vlu.ome_passwd
    except ValueError, ve:
        logger.critical(ve)
        sys.exit(ve)

    kb = KB(driver='omero')(host, user, passwd)
    logging.info('Loading TiterPlate objects')
    plates = kb.get_objects(kb.TiterPlate)
    logging.info('Loaded %d objects' % len(plates))

    with open(args.ofile, 'w') as ofile:
        writer = csv.DictWriter(ofile, CSV_FIELDS, delimiter='\t')
        writer.writeheader()
        for pl in plates:
            logger.debug('Dumping plate %d/%d' % (plates.index(pl) + 1, 
                                                  len(plates)))
            writer.writerow({'label' : pl.label,
                             'barcode' : pl.barcode,
                             'rows' : pl.rows,
                             'columns' : pl.columns,
                             'plate_status' : pl.status.enum_label()})
    logger.info('Job done')
                            
                             


if __name__ == '__main__':
    main(sys.argv[1:])
