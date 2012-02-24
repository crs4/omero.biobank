import sys, argparse, logging, csv

from bl.vl.kb import KnowledgeBase as KB

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

def make_parser():
    parser = argparse.ArgumentParser(description = 'Retrieve GDOs')
    parser.add_argument('--logfile', type = str, help = 'log file (default = stderr)')
    parser.add_argument('--loglevel', type = str, choices = LOG_LEVELS,
                        help = 'logging level', default = 'INFO')
    parser.add_argument('-H', '--host', type = str, help = 'omero hostname',
                        default = 'localhost')
    parser.add_argument('-U', '--user', type = str, help = 'omero user',
                        default = 'root')
    parser.add_argument('-P', '--passwd', type = str, required = True,
                        help = 'omero password')
    parser.add_argument('--marker_set', type = str, help = 'marker set label',
                        default = 'IMMUNO_BC_11419691_B')
    parser.add_argument('--fetch_size', type = int, help = 'number of data set to be loaded',
                        default = 100)
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

    logger.info('Loading GenotypeDataSample objects')
    dsamples = kb.get_objects(kb.GenotypeDataSample)
    logger.info('Loaded %d objects' % len(dsamples))

    logger.info('Loading SNPMarkersSet')
    query = 'SELECT snpm FROM SNPMarkersSet snpm WHERE snpm.label = :mset_label'
    mset = kb.find_all_by_query(query, {'mset_label' : args.marker_set})[0]
    if not mset:
        logger.error('Unable to load SNPMarkersSet with label %s' % args.marker_set)
        sys.exit(2)
    else:
        logger.info('Object loaded')

    gdo_iterator = kb.get_gdo_iterator(mset, dsamples[:args.fetch_size])
    gdos = []

    logger.info('Loading GDOs')
    for gdo in gdo_iterator:
        logger.info(gdo['vid'])
        gdos.append(gdo)
        logger.debug('%d/%d GDOs loaded' % (len(gdos), args.fetch_size))
    logger.info('Loaded %d GDOs' % len(gdos))


if __name__ == '__main__':
    main(sys.argv[1:])
