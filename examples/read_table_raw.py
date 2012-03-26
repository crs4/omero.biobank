import time, logging, argparse, sys

import omero
import omero.model
import omero.rtypes
import omero_Tables_ice

LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

def make_parser():
    parser = argparse.ArgumentParser(description='Check time when reading froma an Omero.Table')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('--host', '-H', type=str, default='localhost',
                        help='omero host')
    parser.add_argument('--user', '-U', type=str, default='root',
                        help='omero user')
    parser.add_argument('--passwd', '-P', type=str, required = True,
                        help='omero password')
    parser.add_argument('--table_name', '-T', type=str, required = True,
                        help='omero.Table name')
    parser.add_argument('--lines', '-L', type=int, default=1000,
                        help='lines that will be read for the speed test')
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

    logger.info('Connecting to %s' % args.host)

    c = omero.client(args.host)
    s = c.createSession(args.user, args.passwd)
    qs = s.getQueryService()

    logger.info('Retrieving table %s' % args.table_name)
    ofile = qs.findByString('OriginalFile', 'name', args.table_name)
    if not ofile:
        logger.error('Unable to find table %s' % args.table_name)
        sys.exit(2)
    logger.info('Table loaded. Table ID is %d' % ofile.id._val)

    r = s.sharedResources()
    t = r.openTable(ofile)

    cols = t.getHeaders()

    logger.info('Start reading %d lines' % args.lines)
    start = time.time()
    for x in xrange(args.lines):
        logger.debug('Reading line %d' % x)
        data = t.read(range(len(cols)), x, x+1)

    logger.info('Read complete in %f' % (time.time() - start))

if __name__ == '__main__':
    main(sys.argv[1:])
