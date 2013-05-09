from bulbs.model import Node, Relationship
from bulbs.property import String
from bulbs.neo4jserver import Graph, Config
from bulbs.config import log as bulbs_log
import httplib2
import time
from bl.vl.utils import get_logger
from bl.vl.utils.ome_utils import ome_hash
import bl.vl.kb.events as events
from bl.vl.graph.errors import DependencyTreeError, MissingEdgeError,\
    MissingNodeError, GraphOutOfSyncError


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
            raise DependencyTreeError('Unable to find Neo4J server at %s' % uri)
        except httplib2.socket.error:
            raise DependencyTreeError('Connection refused by Neo4J server')
        self.graph.add_proxy('ome_objects', OME_Object)
        self.graph.add_proxy('produces', OME_Action)
        self.kb = kb
        if kb:
            self.logger = kb.logger
        else:
            self.logger = get_logger('neo4j-driver')

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

    def create_node(self, obj):
        event = events.build_event(events.NodeCreationEvent, {'bl_obj': obj})
        self.kb.events_sender.send_event(event)

    def save_node(self, node_conf):
        node = self.graph.ome_objects.get_or_create('obj_hash', node_conf['obj_hash'],
                                                    node_conf)
        return node.eid

    def create_edge(self, act, source, dest):
        event = events.build_event(events.EdgeCreationEvent, {'bl_act': act,
                                                              'bl_src_obj': source,
                                                              'bl_dest_obj': dest})
        self.kb.events_sender.send_event(event)

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

    def destroy_node(self, obj):
        event = events.build_event(events.NodeDeletionEvent, {'bl_obj': obj})
        self.kb.events_sender.send_event(event)

    def delete_node(self, node_hash):
        node = self.__get_node_by_hash__(node_hash)
        if node:
            self.graph.vertices.delete(node.eid)
        else:
            raise MissingNodeError('Unable to find node with hash %s. Delete failed.' % node_hash)

    def destroy_edge(self, act):
        event = events.build_event(events.EdgeDeletionEvent, {'bl_act': act})
        self.kb.events_sender.send_event(event)

    def delete_edge(self, edge_hash):
        edge = self.__get_edge_by_hash__(edge_hash)
        if edge:
            self.graph.edges.delete(edge.eid)
        else:
            raise MissingEdgeError('Unable to find edge with hash %s. Delete failed.' % edge_hash)

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

    def __get_node_by_hash__(self, node_hash):
        nodes = list(self.graph.ome_objects.index.lookup(obj_hash=node_hash))
        if len(nodes) == 1:
            return nodes[0]
        elif len(nodes) == 0:
            return None
        else:
            raise DependencyTreeError('Multiple nodes with obj_hash %s' % node_hash)

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