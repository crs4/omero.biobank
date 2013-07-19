import csv, sys, argparse

from bl.vl.utils import LOG_LEVELS, get_logger
from bl.vl.kb import KnowledgeBase as KB
import bl.vl.utils.ome_utils as vlu


def make_parser():
    parser = argparse.ArgumentParser(description='link wells to tubes')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help='omero password')
    parser.add_argument('--out_file', type=str, required=True,
                        help='output file')
    return parser

def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    logger = get_logger("main", level=args.loglevel, filename=args.logfile)

    try:
        host = args.host or vlu.ome_host()
        user = args.user or vlu.ome_user()
        passwd  = args.passwd or vlu.ome_passwd()
    except ValueError, ve:
        logger.critical(ve)
        sys.exit(ve)
        
    kb = KB(driver='omero')(host, user, passwd)

    with open(args.out_file, 'w') as ofile:
        writer = csv.DictWriter(ofile, ['target', 'new_source_type', 
                                        'new_source', 'old_source'],
                                delimiter='\t')
        writer.writeheader()
        logger.info('Loading wells data')
        for w in kb.get_objects(kb.PlateWell):
            if type(w.action.target) == kb.Individual:
                tubes = list(kb.dt.get_connected(w, kb.Tube))
                if len(tubes) == 1:
                    record = {'target' : '%s:%s' % (w.container.label,
                                                    w.label),
                              'new_source_type' : tubes[0].__class__.__name__,
                              'new_source' : tubes[0].label,
                              'old_source' : '%s:%s' % (w.action.target.__class__.__name__,
                                                        w.action.target.id)}
                    writer.writerow(record)
                else:
                    logger.debug('Well %s:%s has %d tubes connected' % (w.container.label,
                                                                        w.label, len(tubes)))
            else:
                logger.debug('Well %s:%s source is %s' % (w.container.label,
                                                          w.label,
                                                          w.action.target.__class__.__name__))
        logger.info('Job completed')
                                 

if __name__ == '__main__':
    main(sys.argv[1:])
