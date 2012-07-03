# Dumps all relationships between an existing Omero.biobank system to
# a neo4j graph database

import sys, argparse, logging

# Used to catch 'connection refused' exception thrown by neo4j server
from httplib2 import socket

from bulbs.model import Node, Relationship
from bulbs.property import String
from bulbs.neo4jserver import Graph, NEO4J_URI, Config

from bl.vl.kb import KnowledgeBase as KB


# Classes used by bulbs to map objects into neo4j
class OME_Object(Node):
    element_type = 'ome_object'

    obj_class = String(nullable=False)
    obj_id    = String(nullable=False)

class OME_Action(Relationship):
    label = 'produces'

    act_type = String(nullable=False)
    act_id   = String(nullable=False)
##################################################


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
    parser.add_argument('-N', '--neo_uri', type=str, help='neo4j server URI')
    return parser

def graph_node(ome_obj, graph, logger):
    node = graph.ome_object.get_or_create('obj_id', ome_obj.id,
                                          {'obj_id' : ome_obj.id,
                                           'obj_class' : type(ome_obj).__name__})
    return node

def get_from_graph(ome_obj, graph, logger):
    nodes = list(graph.ome_object.index.lookup(obj_id=ome_obj.id))
    if len(nodes) == 0:
        return None
    else:
        assert len(nodes) == 1
    node = nodes[0]
    assert str(node.obj_class) == type(ome_obj).__name__
    logger.debug('Retrieved node with ID %s' % node.eid)
    return node
               
def dump_graph_edge(source_node, dest_node, ome_action, graph, logger):
    logger.debug('Build edge %s:%s ---> %s:%s (mapping %s:%s)' % (source_node.obj_class,
                                                                  source_node.obj_id,
                                                                  dest_node.obj_class,
                                                                  dest_node.obj_id,
                                                                  type(ome_action).__name__,
                                                                  ome_action.id))
    e = list(graph.produces.index.lookup(act_id = ome_action.id))
    if len(e) > 0:
        assert len(e) == 1 # ome_action.id must be unique
        assert e[0].outV() == source_node
        assert e[0].inV() == dest_node
        logger.debug('Edge already exists with ID %s' % e[0].eid)
    else:
        graph.produces.create(source_node, dest_node,
                              act_type = type(ome_action).__name__,
                              act_id = ome_action.id)
  

def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    logger = make_logger(args.loglevel, args.logfile)

    kb = KB(driver='omero')(args.host, args.user, args.passwd)

    try:
        logger.debug('Connecting to graph')
        if args.neo_uri:
            gconf = Config(args.neo_uri)
            graph = Graph(gconf)
        else:
            graph = Graph()
        print logger.handlers
        logger.debug('Connected to graph with URI %s' % NEO4J_URI)
        graph.add_proxy('ome_object', OME_Object)
        graph.add_proxy('produces', OME_Action)
    except socket.error:
        msg = 'Connection refused by neo4j server'
        logger.critical(msg)
        sys.exit(msg)

    ome_obj_classes = [kb.Individual, kb.Vessel, kb.DataSample,
                       kb.VLCollection]
    known_objects = []
    for ooc in ome_obj_classes:
        logger.info('Loading %s objects (and subclasses)' % ooc.__name__)
        objs = kb.get_objects(ooc)
        logger.info('Loaded %d items' % len(objs))
        logger.info('Dumping to graph')
        for o in objs:
            logger.debug('Dumping object %d/%d' % (objs.index(o)+1, 
                                                  len(objs)))
            n = graph_node(o, graph, logger)
        logger.info('Dump completed')
        known_objects.extend(objs)

    logger.info('Loading Action objects')
    acts = kb.get_objects(kb.Action)
    logger.info('Loaded %d items' % len(acts))

    logger.info('Building graph edges')
    for o in known_objects:
        if hasattr(o.action, 'target'):
            o_node = get_from_graph(o, graph, logger)
            act = o.action
            if type(act) in [kb.ActionOnDataCollectionItem]:
                src = act.target.dataSample
            else:
                src = act.target
            o_src = get_from_graph(src, graph, logger)
            dump_graph_edge(o_src, o_node, act, graph, logger)

    logger.info('Job completed')

if __name__ == '__main__':
    main(sys.argv[1:])
