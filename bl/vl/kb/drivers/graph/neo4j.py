from bulbs.model import Node, Relationship
from bulbs.property import String
from bulbs.neo4jserver import Graph, Config
from bulbs.config import log as bulbs_log
import httplib2


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


class MissingNodeError(Exception):
    pass


class MissingEdgeError(Exception):
    pass


class Neo4JDriver(object):

    DIRECTION_INCOMING = 1
    DIRECTION_OUTGOING = 2
    DIRECTION_BOTH = 3

    def __init__(self, uri, username=None, password=None, kb=None):
        graph_conf = Config(uri, username, password)
        try:
            self.graph = Graph(graph_conf)
            for h in bulbs_log.root.handlers:
                bulbs_log.root.removeHandler(h)
        except httplib2.ServerNotFoundError:
            raise DependencyTreeError('Unable to find Neo4J server at %s' % uri)
        except httplib2.socket.error:
            raise DependencyTreeError('Connection refused by Neo4J server')
        self.graph.add_proxy('ome_objects', OME_Object)
        self.graph.add_proxy('produces', OME_Action)
        self.kb = None

    def __get_node_by_hash__(self, node_hash):
        nodes = list(self.graph.ome_objects.index.lookup(obj_hash=node_hash))
        if len(nodes) == 1:
            return nodes[0]
        elif len(nodes) == 0:
            return None
        else:
            raise DependencyTreeError('Multiple nodes with hash %s' % node_hash)

    def __get_edge_by_hash__(self, edge_hash):
        edges = list(self.graph.produces.index.lookup(act_hash=edge_hash))
        if len(edges) == 1:
            return edges[0]
        elif len(edges) == 0:
            return None
        else:
            raise DependencyTreeError('Multiple edges with hash %s' % edge_hash)

    def save_node(self, node_conf):
        node = self.graph.ome_objects.get_or_create('obj_hash', node_conf['obj_hash'],
                                                    node_conf)
        return node.eid

    def save_edge(self, action_conf, source_hash, dest_hash):
        edge = self.__get_edge_by_hash__(action_conf['act_hash'])
        if not edge:
            src_node = self.__get_node_by_hash__(source_hash)
            if not src_node:
                raise MissingNodeError('No node with hash %s' % source_hash)
            dest_node = self.__get_node_by_hash__(dest_hash)
            if not dest_node:
                raise MissingNodeError('No node with hash %s' % dest_hash)
            edge = self.graph.produces.create(src_node, dest_node, action_conf)
        return edge.eid

    def delete_node(self, node_hash):
        node = self.__get_node_by_hash__(node_hash)
        if node:
            self.graph.vertices.delete(node.eid)
        else:
            raise MissingNodeError('Unable to find node with hash %s. Delete failed.' % node_hash)

    def delete_edge(self, edge_hash):
        edge = self.__get_edge_by_hash__(edge_hash)
        if edge:
            self.graph.edges.delete(edge.eid)
        else:
            raise MissingEdgeError('Unable to find edge with hash %s. Delete failed.' % edge_hash)

    def update_edge(self, action_hash, new_source_hash, new_dest_hash):
        raise NotImplementedError()

    def get_connected(self, obj, aklass=None, direction=DIRECTION_BOTH,
                      query_depth=None):
        raise NotImplementedError()

    def get_connected_infos(self, obj, aklass=None, direction=DIRECTION_BOTH,
                            query_depth=None):
        raise NotImplementedError()