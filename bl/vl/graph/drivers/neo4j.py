from bulbs.model import Node, Relationship
from bulbs.property import String
from bulbs.neo4jserver import Graph, Config
from bulbs.config import log as bulbs_log
import httplib2
import time
from bl.vl.utils import get_logger
from bl.vl.utils.ome_utils import ome_hash
from bl.vl.utils.graph import build_edge_id
import bl.vl.kb.events as events
from bl.vl.graph.errors import DependencyTreeError, MissingEdgeError,\
    MissingNodeError, GraphOutOfSyncError, GraphAuthenticationError, \
    GraphConnectionError


class OME_Object(Node):

    element_type = 'ome_object'

    obj_class = String(nullable=False)
    obj_id = String(nullable=False)
    obj_hash = String(nullable=False)

    def __hash__(self):
        return self.eid


class OME_Action(Relationship):

    label = 'produces'

    edge_id = String(nullable=False)
    act_type = String(nullable=False)
    act_id = String(nullable=False)
    act_hash = String(nullable=False)

    def __hash__(self):
        return self.eid


class Neo4JDriver(object):

    BLOCKED_PROXY_CALLBACKS = (
        'save_node',
        'save_edge',
        'delete_node',
        'delete_edge',
        'update_edge',
    )

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
            raise GraphConnectionError('Unable to find Neo4j server at %s' % uri)
        except httplib2.socket.error:
            raise GraphConnectionError('Connection refused by Neo4j server')
        except TypeError:
            # Using plugin from https://github.com/neo4j-contrib/authentication-extension to manage authentication
            # will produce a TypeError if no username and password are given or if given authentication credentials
            # are wrong
            if not username or not password:
                msg = 'Unable to connect to Neo4j server, authentication required'
            else:
                msg = 'Unable to connect to Neo4j server, wrong username and\or password'
            raise GraphAuthenticationError(msg)
        self.graph.add_proxy('ome_objects', OME_Object)
        self.graph.add_proxy('produces', OME_Action)
        self.kb = kb
        if kb:
            self.logger = kb.logger
        else:
            self.logger = get_logger('neo4j-driver')

    def __get_node_by_hash__(self, node_hash):
        try:
            nodes = list(self.graph.ome_objects.index.lookup(obj_hash=node_hash))
        except httplib2.socket.error:
            raise GraphConnectionError('Connection to Neo4j server ended unexpectedly')
        if len(nodes) == 1:
            return nodes[0]
        elif len(nodes) == 0:
            return None
        else:
            raise DependencyTreeError('Multiple nodes with hash %s' % node_hash)

    def __get_edges_by_hash__(self, edge_hash):
        try:
            edges = list(self.graph.produces.index.lookup(act_hash=edge_hash))
        except httplib2.socket.error:
            raise GraphConnectionError('Connection to Neo4j server ended unexpectedly')
        except TypeError:
            return None
        if len(edges) == 0:
            return None
        else:
            return edges

    def __get_edge_by_id__(self, edge_id):
        try:
            edges = list(self.graph.produces.index(edge_id=edge_id))
        except httplib2.socket.error:
            raise GraphConnectionError('Connection to Neo4j server ended unexpectedly')
        except TypeError:
            return None
        if len(edges) == 0:
            return None
        elif len(edges) == 1:
            return edges[0]
        else:
            raise DependencyTreeError('Multiple edges with ID %s' % edge_id)

    def __get_edge_by_nodes__(self, src_node_hash, dest_node_hash):
        return self.__get_edge_by_id__(build_edge_id(src_node_hash, dest_node_hash))

    def create_node(self, obj):
        event = events.build_event(events.NodeCreationEvent, {'bl_obj': obj})
        self.kb.events_sender.send_event(event)

    def save_node(self, node_conf):
        try:
            node = self.graph.ome_objects.get_or_create('obj_hash', node_conf['obj_hash'],
                                                        node_conf)
        except httplib2.socket.error:
            raise GraphConnectionError('Connection to Neo4j server ended unexpectedly')
        return node.eid

    def create_edge(self, act, source, dest):
        event = events.build_event(events.EdgeCreationEvent, {'bl_act': act,
                                                              'bl_src_obj': source,
                                                              'bl_dest_obj': dest})
        self.kb.events_sender.send_event(event)

    def save_edge(self, action_conf, source_hash, dest_hash):
        edge = self.__get_edge_by_nodes__(source_hash, dest_hash)
        if not edge:
            src_node = self.__get_node_by_hash__(source_hash)
            if not src_node:
                raise MissingNodeError('No node with hash %s' % source_hash)
            dest_node = self.__get_node_by_hash__(dest_hash)
            if not dest_node:
                raise MissingNodeError('No node with hash %s' % dest_hash)
            try:
                edge = self.graph.produces.create(src_node, dest_node, action_conf)
            except httplib2.socket.error:
                raise GraphConnectionError('Connection to Neo4j server ended unexpectedly')
        return edge.eid

    def destroy_node(self, obj):
        event = events.build_event(events.NodeDeletionEvent, {'bl_obj': obj})
        self.kb.events_sender.send_event(event)

    def delete_node(self, node_hash):
        node = self.__get_node_by_hash__(node_hash)
        if node:
            try:
                self.graph.vertices.delete(node.eid)
            except httplib2.socket.error:
                raise GraphConnectionError('Connection to Neo4j server ended unexpectedly')
        else:
            raise MissingNodeError('Unable to find node with hash %s. Delete failed.' % node_hash)

    def destroy_edge(self, src, dest):
        event = events.build_event(events.EdgeDeletionEvent, {'src': src,
                                                              'dest': dest})
        self.kb.events_sender.send_event(event)

    def delete_edge(self, edge_id):
        edge = self.__get_edge_by_id__(edge_id)
        if edge:
            try:
                self.graph.edges.delete(edge.eid)
            except httplib2.socket.error:
                raise GraphConnectionError('Connection to Neo4j server ended unexpectedly')
        else:
            raise MissingEdgeError('Unable to find edge with ID %s. Delete failed.' % edge_id)

    def destroy_edges(self, act):
        event = events.build_event(events.EdgesDeletionEvent, {'bl_act': act})
        self.kb.events_sender.send_event(event)

    def delete_edges(self, edge_hash):
        edges = self.__get_edges_by_hash__(edge_hash)
        if edges:
            for e in edges:
                self.graph.edges.delete(e.eid)
        else:
            raise MissingEdgeError('Unable to find edges with hash %s. Delete failed.' % edge_hash)

    def modify_edge(self, act, source=None, dest=None):
        if source is None and dest is None:
            raise ValueError('no new source or destination specified, no edge update can be triggered')
        event = events.build_event(events.EdgeUpdateEvent, {'bl_act': act,
                                                            'bl_src_obj': source,
                                                            'bl_dest_obj': dest})
        self.kb.events_sender.send_event(event)

    def update_edge(self, action_hash, new_source_hash, new_dest_hash):
        raise NotImplementedError()

    def __get_node__(self, obj):
        return self.__get_node_by_hash__(ome_hash(obj.ome_obj))

    def __get_ome_obj__(self, node):
        try:
            return self.kb._CACHE[node.obj_hash]
        except KeyError:
            return self.kb.get_by_vid(getattr(self.kb, node.obj_class),
                                      str(node.obj_id))

    def __get_ome_obj_by_info__(self, obj_info):
        try:
            return self.kb._CACHE[obj_info['object_hash']]
        except KeyError:
            return self.kb.get_by_vid(getattr(self.kb, obj_info['object_type']),
                                      obj_info['object_id'])

    def __check_queue_status__(self, wait_interval, max_attempts):
        attempts_count = 0
        if self.kb and self.kb.events_sender:
            while attempts_count < max_attempts:
                self.logger.debug('Checking queue status, attempt %d/%d' % (attempts_count + 1,
                                                                            max_attempts))
                if not self.kb.events_sender.is_queue_empty:
                    attempts_count += 1
                    time.sleep(wait_interval)
                    self.logger.debug('Sleep for %d seconds' % wait_interval)
                else:
                    return
            raise GraphOutOfSyncError('Queue is not empty, graph could be out of sync')
        else:
            raise DependencyTreeError('No proper events handler configured, unable to check queue status')

    def __get_connected_nodes__(self, node, direction, depth, visited_nodes=None):
        if not visited_nodes:
            visited_nodes = set()
        if direction not in (self.DIRECTION_INCOMING, self.DIRECTION_OUTGOING,
                             self.DIRECTION_BOTH):
            raise DependencyTreeError('Not a valid direction for graph traversal')
        if depth == 0:
            visited_nodes.add(node)
            return visited_nodes
        try:
            if direction == self.DIRECTION_INCOMING:
                connected = list(node.inV('produces'))
            elif direction == self.DIRECTION_OUTGOING:
                connected = list(node.outV('produces'))
            elif direction == self.DIRECTION_BOTH:
                connected = list(node.bothV('produces'))
        except httplib2.socket.error:
            raise GraphConnectionError('Connection to Neo4j server ended unexpectedly')
        visited_nodes.add(node)
        connected = set(connected) - visited_nodes
        if len(connected) == 0:
            return visited_nodes
        else:
            if depth:
                depth -= 1
            return set.union(*[self.__get_connected_nodes__(cn, direction, depth, visited_nodes)
                               for cn in connected])

    def get_connected_infos(self, obj, aklass=None, direction=DIRECTION_BOTH, query_depth=None,
                            wait_interval=5, max_attempts=3):
        self.__check_queue_status__(wait_interval, max_attempts)
        obj_node = self.__get_node__(obj)
        if not obj_node:
            raise DependencyTreeError('Unable to retrieve a node for object %s:%s' %
                                      (type(obj).__name__, obj.id))
        try:
            connected_nodes = self.__get_connected_nodes__(obj_node, direction, query_depth)
        except RuntimeError, re:
            raise DependencyTreeError(re)
        if aklass:
            return [
                {
                    'object_type': str(cn.obj_class),
                    'object_id': str(cn.obj_id),
                    'object_hash': str(cn.obj_hash)
                }
                for cn in connected_nodes
                if cn.obj_class == aklass.OME_TABLE
            ]
        else:
            return [
                {
                    'object_type': str(cn.obj_class),
                    'object_id': str(cn.obj_id),
                    'object_hash': str(cn.obj_hash)
                }
                for cn in connected_nodes
            ]

    def get_connected(self, obj, aklass=None, direction=DIRECTION_BOTH,
                      query_depth=None):
        connected_nodes_infos = self.get_connected_infos(obj, aklass, direction,
                                                         query_depth)
        return [self.__get_ome_obj_by_info__(cni) for cni in connected_nodes_infos]