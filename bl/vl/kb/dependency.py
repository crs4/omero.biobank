from bulbs.model import Node, Relationship
from bulbs.property import String
from bulbs.neo4jserver import Graph, Config
from bulbs.config import log as bulbs_log

# Used to capture neo4j connection exceptions
import httplib2

import config as vlconf
from bl.vl.kb.drivers.omero.utils import ome_hash


class OME_Object(Node):
    element_type = 'ome_object'
    
    obj_class = String(nullable=False)
    obj_id = String(nullable=False)
    obj_hash = String(nullable=False)

    def __hash__(self):
        return self.eid


class OME_Action(Relationship):
    label = 'produces'
    act_type = String(nullable=False)
    act_id = String(nullable=False)
    act_hash = String(nullable=False)

    def __hash__(self):
        return self.eid


class DependencyTreeError(Exception):
    pass
    

class DependencyTree(object):

    DIRECTION_INCOMING = 1
    DIRECTION_OUTGOING = 2
    DIRECTION_BOTH     = 3

    def __init__(self, kb):
        self.kb = kb
        gconf = Config(vlconf.NEO4J_URI)
        try:
            self.graph = Graph(gconf)
            # Resolve the logger issue
            for h in bulbs_log.root.handlers:
                bulbs_log.root.removeHandler(h)
        except httplib2.ServerNotFoundError:
            raise DependencyTreeError('Unable to find Node4J server at %s' % 
                                      vlconf.NEO4J_URI)
        except httplib2.socket.error:
            raise DependencyTreeError('Connection refused by Node4J server')
        self.graph.add_proxy('ome_objects', OME_Object)
        self.graph.add_proxy('produces', OME_Action)
        if not self.do_consistency_check():
            raise DependencyTreeError('Graph out-of-sync!')


    # TODO: do a consistency check against OMERO in order to check that
    # everything inside the Graph is in sync
    def do_consistency_check(self):
        return True

    def dump_node(self, obj):
        self.__save_node__({'obj_class' : type(obj).__name__,
                            'obj_id' : obj.id,
                            'obj_hash' : ome_hash(obj.ome_obj)})

    def dump_edge(self, source, dest, act):
        self.__save_edge__({'act_type' : type(act).__name__,
                            'act_id' : act.id,
                            'act_hash' : ome_hash(act.ome_obj)},
                           ome_hash(source.ome_obj),
                           ome_hash(dest.ome_obj))

    def drop_node(self, obj):
        node = self.__get_node__(obj)
        if node:
            self.graph.vertices.delete(node.eid)

    def drop_edge(self, act):
        edge = self.__get_edge__(act)
        if edge:
            self.graph.edges.delete(edge.eid)

    def __save_node__(self, node_conf):
        self.graph.ome_objects.get_or_create('obj_hash', node_conf['obj_hash'],
                                             node_conf)

    def __save_edge__(self, edge_conf, src_hash, dest_hash):
        edge = self.get_edge_by_hash__(edge_conf['act_hash'])
        if not edge:
            src_node = self.__get_node_by_hash__(src_hash)
            if not src_node:
                raise DependencyTreeError('Unmapped source node, unable to create the edge')
            dest_node = self.__get_node_by_hash__(dest_hash)
            if not dest_node:
                raise DependencyTreeError('Unmapped destinantion node, unable to create the edge')
            self.graph.produces.create(src_node, dest_node, **edge_conf)

    def __get_node__(self, obj):
        return self.__get_node_by_hash__(ome_hash(obj.ome_obj))

    def __get_edge__(self, act):
        return self.__get_edge_by_hash__(ome_hash(act.ome_obj))

    def __get_node_by_hash__(self, node_hash):
        nodes = list(self.graph.ome_objects.index.lookup(obj_hash = node_hash))
        if len(nodes) == 1:
            return nodes[0]
        elif len(nodes) == 0:
            return None
        else:
            raise DependencyTreeError('Multiple nodes with obj_hash = %s' % node_hash)

    def __get_edge_by_hash__(self, edge_hash):
        edges = list(self.graph.produces.index.lookup(act_hash = edge_hash))
        if len(edges) == 1:
            return edges[0]
        elif len(edges) == 0:
            return None
        else:
            raise DependencyTreeError('Multiple edges with edge_hash = %s' % edge_hash)

    def __get_ome_obj__(self, node):
        try:
            return self.kb._CACHE[node.obj_hash]
        except KeyError, ke:
            return self.kb.get_by_vid(getattr(self.kb, node.obj_class),
                                      str(node.obj_id))

    def __get_ome_obj_by_info__(self, obj_info):
        try:
            return self.kb._CACHE[obj_info['object_hash']]
        except KeyError, ke:
            return self.kb.get_by_vid(getattr(self.kb, obj_info['object_type']),
                                      obj_info['object_id'])

    def __get_ome_action__(self, edge):
        try:
            return self.kb._CACHE(edge.act_hash)
        except KeyError, ke:
            return self.kb.get_by_vid(edge.act_class, edge.act_hash)

    def __get_connected_nodes__(self, node, direction, depth,
                                visited_nodes = None):
        if not visited_nodes:
            visited_nodes = set()
        if direction not in (self.DIRECTION_INCOMING,
                             self.DIRECTION_OUTGOING,
                             self.DIRECTION_BOTH):
            raise DependencyTreeError('Not a valid direction for graph traversal')
        if depth == 0:
            visited_nodes.add(node)
            return visited_nodes
        if direction == self.DIRECTION_INCOMING:
            connected = list(node.inV('produces'))
        elif direction == self.DIRECTION_OUTGOING:
            connected = list(node.outV('produces'))
        elif direction == self.DIRECTION_BOTH:
            connected = list(node.bothV('produces'))
        visited_nodes.add(node)
        connected = set(connected) - visited_nodes
        if len(connected) == 0:
            return visited_nodes
        else:
            if not depth is None:
                depth = depth - 1
            return set.union(*[self.__get_connected_nodes__(cn, direction, depth, visited_nodes)
                               for cn in connected])

    def get_connected_infos(self, ome_obj, aklass=None,
                            direction = DIRECTION_BOTH, query_depth = None):
        obj_node = self.__get_node__(ome_obj)
        if not obj_node:
            raise DependencyTreeError('Unable to find object %s:%s into the graph' %
                                      (type(ome_obj).__name__, ome_obj.id))
        try:
            connected_nodes = self.__get_connected_nodes__(obj_node, direction, query_depth)
        except RuntimeError, re:
            raise DependencyTreeError(re)
        if aklass:
            return [{'object_type' : str(cn.obj_class),
                     'object_id'   : str(cn.obj_id),
                     'object_hash' : str(cn.obj_hash)} for cn in connected_nodes
                    if cn.obj_class == aklass]
        else:
            return[{'object_type' : str(cn.obj_class),
                    'object_id'   : str(cn.obj_id),
                    'object_hash' : str(cn.obj_hash)} for cn in connected_nodes]

    def get_connected(self, ome_obj, aklass=None,
                      direction = DIRECTION_BOTH, query_depth = None):
        connected_nodes_infos = self.get_connected_infos(ome_obj, aklass, direction,
                                                         query_depth)
        return [self.__get_ome_obj_by_info__(cni) for cni in connected_nodes_infos]
