# Import data exported using build_graph_input.py into the neo4j
# database

import sys, argparse, logging, csv

from bl.vl.kb import KnowledgeBase as KB

LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

def make_logger(level_str='INFO', filename=None):
    formatter = logging.Formatter(
        fmt = '%(asctime)s|%(levelname)-8s|%(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S'
        )
    logger = logging.getLogger(__name__)
    for h in logger.handlers:
        logger.removeHandler(h)
    if filename:
        handler = logging.FileHandler(filename, 'w')
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level_str))
    return logger

def make_parser():
    parser = argparse.ArgumentParser(description='Write data to neo4j database')
    parser.add_argument('--logfile', type=str, help='log file (default = stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname',
                        required=True)
    parser.add_argument('-U', '--user', type=str, help='omero user',
                        required=True)
    parser.add_argument('-P', '--passwd', type=str, help='omero password',
                        required=True)
    parser.add_argument('--nodes_file', type=str, help='nodes description tsv file',
                        required=True)
    parser.add_argument('--edges_file', type=str, help='edges description tsv file',
                        required=True)
    return parser


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    logger = make_logger(args.loglevel, args.logfile)

    kb = KB(driver='omero')(args.host, args.user, args.passwd)
    kb.update_dependency_tree()

    with open(args.nodes_file) as f:
        reader = csv.DictReader(f, delimiter='\t')
        logger.info('Start dumping nodes')
        rows = list(reader)
        for row in rows:
            logger.debug('Dumping record %d/%d --- %s:%s' % (rows.index(row) + 1, len(rows),
                                                             row['obj_class'], row['obj_id']))
            kb.dt._save_node({'obj_class' : row['obj_class'],
                              'obj_id' : row['obj_id'],
                              'obj_hash' : row['obj_hash']})
    logger.info('Done dumping nodes')

    with open(args.edges_file) as f:
        reader = csv.DictReader(f, delimiter='\t')
        logger.info('Start dumping edges')
        rows = list(reader)
        for row in rows:
            logger.debug('Dumping record %d/%d --- %s:%s' % (rows.index(row) + 1, len(rows),
                                                             row['act_type'], row['act_id']))
            kb.dt._save_edge({'act_type' : row['act_type'],
                              'act_id' : row['act_id'],
                              'act_hash' : row['act_hash']},
                             row['source'], row['dest'])
    logger.info('Done dumping edges')

if __name__ == '__main__':
    main(sys.argv[1:])
