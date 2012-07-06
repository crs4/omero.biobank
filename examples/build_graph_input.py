# Dumps OMERO.biobank objects and actions in two different CSV files
# used to create nodes and edges into the neo4j database

import sys, argparse, logging, csv

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.kb.drivers.omero.utils import ome_hash

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
    parser = argparse.ArgumentParser(description='Load relationships from a given OMERO.biobank running server and dump them to a neo4j server as a graph')
    parser.add_argument('--logfile', type=str, help='log file (default = stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname',
                        required=True)
    parser.add_argument('-U', '--user', type=str, help='omero user',
                        required=True)
    parser.add_argument('-P', '--passwd', type=str, help='omero password',
                        required=True)
    parser.add_argument('--nodes_file', type=str, help='file where nodes data will be stored',
                        required=True)
    parser.add_argument('--edges_file', type=str, help='file where edges data will be stored',
                        required=True)
    return parser

def get_nodes(kb, logger):
    node_classes = [kb.Individual, kb.Vessel, kb.DataSample,
                    kb.VLCollection]
    nodes = []
    for nc in node_classes:
        logger.info('Loading %s objects' % nc.__name__)
        objs = kb.get_objects(nc)
        logger.info('Loaded %d objects' % len(objs))
        nodes.extend(objs)
    return nodes

def dump_node(obj, ofile):
    ofile.writerow({'obj_class' : type(obj).__name__,
                    'obj_id' : obj.id,
                    'obj_hash' : ome_hash(obj.ome_obj)})

def dump_edge(source, dest, act, ofile):
    ofile.writerow({'act_class' : type(act).__name__,
                    'act_id' : act.id,
                    'act_hash' : ome_hash(act.ome_obj),
                    'source' : ome_hash(source.ome_obj),
                    'dest' : ome_hash(dest.ome_obj)})


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    logger = make_logger(args.loglevel, args.logfile)

    kb = KB(driver='omero')(args.host, args.user, args.passwd)

    nodes = get_nodes(kb, logger)
    with open(args.nodes_file, 'w') as ofile:
        writer = csv.DictWriter(ofile, ['obj_class', 'obj_id', 'obj_hash'],
                                delimiter='\t')
        writer.writeheader()
        logger.info('Start dumping nodes to file %s' % args.nodes_file)
        for n in nodes:
            dump_node(n, writer)
    logger.info('Done dumping nodes')


    logger.info('Loading actions')
    acts = kb.get_objects(kb.Action)
    logger.info('Loaded %d actions' % len(acts))

    logger.debug('Caching DataCollectionItem objects')
    dcobjs = kb.get_objects(kb.DataCollectionItem)
    logger.debug('Loaded %d objects' % len(dcobjs))

    with open(args.edges_file, 'w') as ofile:
        writer = csv.DictWriter(ofile, ['act_class', 'act_id', 'act_hash', 'source', 'dest'],
                                delimiter='\t')
        writer.writeheader()
        logger.info('Start dumping edges to file %s' % args.edges_file)
        for n in nodes:
            if hasattr(n.action, 'target'):
                act = n.action
                if type(act) in [kb.ActionOnDataCollectionItem]:
                    src = act.target.dataSample
                else:
                    src = act.target
                dump_edge(src, n, act, writer)
    logger.info('Done dumping edges')

if __name__ == '__main__':
    main(sys.argv[1:])
