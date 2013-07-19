import time, argparse, sys

import omero
import omero.model
import omero.rtypes
import omero_Tables_ice

from bl.vl.utils import LOG_LEVELS, get_logger
import tables


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
    parser.add_argument('--file_path', '-F', type=str, default=None,
                        help='filen used for the low-level test usint pytables libs (default=None, no low-level test will be performed')
    return parser
    
def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    logger = get_logger("main", level=args.loglevel, filename=args.logfile)

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

    logger.info('Read completed in %f' % (time.time() - start))

    if args.file_path:
        start = time.time()
        table = tables.openFile(args.file_path)
        logger.info('Start reading %d lines using pytables API' % args.lines)
        for x in xrange(args.lines):
            logger.debug('Reading line %d' % x)
            data = table.root.OME.Measurements.read(x, x+1)
        logger.info('Read completed in %f' % (time.time() - start))

if __name__ == '__main__':
    main(sys.argv[1:])
