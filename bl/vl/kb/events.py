import json
from voluptuous import Schema, MultipleInvalid, Invalid
from bl.vl.utils import decode_dict
from bl.vl.utils.ome_utils import ome_hash
from bl.vl.utils.graph import build_edge_id


def build_event(event_cls, event_conf):

    def get_node_creation_data(conf):
        return {
            'action': 'NODE_CREATE',
            'details': {
                'obj_class': type(conf['bl_obj']).__name__,
                'obj_id': conf['bl_obj'].id,
                'obj_hash': ome_hash(conf['bl_obj'].ome_obj)
            }
        }

    def get_edge_creation_data(conf):
        return {
            'action': 'EDGE_CREATE',
            'details': {
                'edge_id': build_edge_id(ome_hash(conf['bl_src_obj'].ome_obj),
                                         ome_hash(conf['bl_dest_obj'].ome_obj)),
                'act_type': type(conf['bl_act']).__name__,
                'act_id': conf['bl_act'].id,
                'act_hash': ome_hash(conf['bl_act'].ome_obj)
            },
            'source_node': ome_hash(conf['bl_src_obj'].ome_obj),
            'dest_node': ome_hash(conf['bl_dest_obj'].ome_obj)
        }

    def get_node_deletion_data(conf):
        return {
            'action': 'NODE_DELETE',
            'target': ome_hash(conf['bl_obj'].ome_obj)
        }

    def get_edge_deletion_data(conf):
        return {
            'action': 'EDGE_DELETE',
            'target': build_edge_id(ome_hash(conf['src'].ome_obj),
                                    ome_hash(conf['dest'].ome_obj))
        }

    def get_edges_deletion_data(conf):
        return {
            'action': 'EDGES_DELETE',
            'target': ome_hash(conf['bl_act'].ome_obj)
        }

    def get_edge_update_data(conf):
        data = {
            'action': 'EDGE_UPDATE',
            'target': ome_hash(conf['bl_act'].ome_obj),
            'new_source_node': None,
            'new_dest_node': None
        }
        if conf['bl_src_obj']:
            data['new_source_node'] = ome_hash(conf['bl_src_obj'].ome_obj)
        if conf['bl_dest_obj']:
            data['new_dest_node'] = ome_hash(conf['bl_dest_obj'].ome_obj)
        return data

    get_data_map = {
        NodeCreationEvent: get_node_creation_data,
        EdgeCreationEvent: get_edge_creation_data,
        NodeDeletionEvent: get_node_deletion_data,
        EdgeDeletionEvent: get_edge_deletion_data,
        EdgesDeletionEvent: get_edges_deletion_data,
        EdgeUpdateEvent: get_edge_update_data,
    }

    event = event_cls(get_data_map[event_cls](event_conf))
    event.validate()
    return event


def decode_event(routing_key, msg_body):
    decode_map = {
        'graph.node.create': NodeCreationEvent,
        'graph.edge.create': EdgeCreationEvent,
        'graph.node.delete': NodeDeletionEvent,
        'graph.edge.delete': EdgeDeletionEvent,
        'graph.edges.delete': EdgesDeletionEvent,
        'graph.edge.update': EdgeUpdateEvent,
    }
    decode_key = '.'.join(routing_key.split('.')[-3:])
    event = decode_map[decode_key](json.loads(msg_body, object_hook=decode_dict))
    event.validate()
    return event


class InvalidMessageError(Exception):
    pass


class BasicEvent(object):

    def __init__(self, event_type, data=None):
        self.event_type = event_type
        self.data = data

    @property
    def msg(self):
        return json.dumps(self.data)

    def validate(self, schema):
        try:
            schema(self.data)
        except (MultipleInvalid, Invalid):
            raise InvalidMessageError('Unknown or invalid message structure')


class NodeCreationEvent(BasicEvent):

    def __init__(self, data):
        super(NodeCreationEvent, self).__init__('graph.node.create', data)

    def validate(self):
        schema = Schema(
            {
                'action': 'NODE_CREATE',
                'details': {
                    'obj_class': str,
                    'obj_id': str,
                    'obj_hash': int
                }
            }
        )
        super(NodeCreationEvent, self).validate(schema)


class EdgeCreationEvent(BasicEvent):

    def __init__(self, data):
        super(EdgeCreationEvent, self).__init__('graph.edge.create', data)

    def validate(self):
        schema = Schema(
            {
                'action': 'EDGE_CREATE',
                'details': {
                    'edge_id': str,
                    'act_type': str,
                    'act_id': str,
                    'act_hash': int
                },
                'source_node': int,
                'dest_node': int
            }
        )
        super(EdgeCreationEvent, self).validate(schema)


class NodeDeletionEvent(BasicEvent):

    def __init__(self, data):
        super(NodeDeletionEvent, self).__init__('graph.node.delete', data)

    def validate(self):
        schema = Schema(
            {
                'action': 'NODE_DELETE',
                'target': int
            }
        )
        super(NodeDeletionEvent, self).validate(schema)


class EdgeDeletionEvent(BasicEvent):

    def __init__(self, data):
        super(EdgeDeletionEvent, self).__init__('graph.edge.delete', data)

    def validate(self):
        schema = Schema(
            {
                'action': 'EDGE_DELETE',
                'target': str
            }
        )
        super(EdgeDeletionEvent, self).validate(schema)


class EdgesDeletionEvent(BasicEvent):

    def __init__(self, data):
        super(EdgesDeletionEvent, self).__init__('graph.edges.delete', data)

    def validate(self):
        schema = Schema(
            {
                'action': 'EDGES_DELETE',
                'target': int
            }
        )
        super(EdgesDeletionEvent, self).validate(schema)


class EdgeUpdateEvent(BasicEvent):

    def __init__(self, data):
        super(EdgeUpdateEvent, self).__init__('graph.edge.update', data)

    def validate(self):
        def validate_optional_int(value):
            if value is not None and type(value) != int:
                raise ValueError('not integer or None')
        schema = Schema(
            {
                'action': 'EDGE_UPDATE',
                'target': int,
                'new_source_node': validate_optional_int,
                'new_dest_node': validate_optional_int
            }
        )
        super(EdgeUpdateEvent, self).validate(schema)
