import json
from bl.vl.kb.drivers.omero.utils import ome_hash


class BasicEvent(object):

    def __init__(self, event_type, data=None):
        self.event_type = event_type
        self.data = data

    @property
    def msg(self):
        return json.dumps(self.data)


class NodeCreationEvent(BasicEvent):

    def __init__(self, bl_obj=None):
        super(NodeCreationEvent, self).__init__('graph.node.create')
        if bl_obj:
            data = {
                'action': 'NODE_CREATE',
                'details': {
                    'obj_class': type(bl_obj).__name__,
                    'obj_id': bl_obj.id,
                    'obj_hash': ome_hash(bl_obj.ome_obj)
                }
            }
            self.data = data


class EdgeCreationEvent(BasicEvent):

    def __init__(self, bl_act=None, bl_src_obj=None, bl_dest_obj=None):
        super(EdgeCreationEvent, self).__init__('graph.edge.create')
        if bl_act and bl_src_obj and bl_dest_obj:
            data = {
                'action': 'EDGE_CREATE',
                'details': {
                    'act_type': type(bl_act).__name__,
                    'act_id': bl_act.id,
                    'act_hash': ome_hash(bl_act.ome_obj)
                },
                'source_node': ome_hash(bl_src_obj.ome_obj),
                'dest_node': ome_hash(bl_dest_obj.ome_obj)
            }
            self.data = data


class NodeDeletionEvent(BasicEvent):

    def __init__(self, bl_obj=None):
        super(NodeDeletionEvent, self).__init__('graph.node.delete')
        if bl_obj:
            data = {
                'action': 'NODE_DELETE',
                'target': ome_hash(bl_obj.ome_obj)
            }
            self.data = data


class EdgeDeletionEvent(BasicEvent):

    def __init__(self, bl_act=None):
        super(EdgeDeletionEvent, self).__init__('graph.edge.delete')
        if bl_act:
            data = {
                'action': 'EDGE_DELETE',
                'target': ome_hash(bl_act.ome_obj)
            }
            self.data = data


class EdgeUpdateEvent(BasicEvent):

    def __init__(self, bl_act=None, bl_src_obj=None, bl_dest_obj=None):
        super(EdgeUpdateEvent, self).__init__('graph.edge.update')
        if bl_act:
            data = {
                'action': 'EDGE_UPDATE',
                'target': ome_hash(bl_act.ome_obj),
                'new_source_node': None,
                'new_dest_node': None
            }
            if bl_src_obj:
                data['new_source_node'] = ome_hash(bl_src_obj.ome_obj)
            if bl_dest_obj:
                data['new_dest_node'] = ome_hash(bl_dest_obj.ome_obj)
            self.data = data


class EventsDecoder(object):

    def __init__(self):
        pass

    @staticmethod
    def decode(routing_key, msg_body):
        decode_map = {
            'graph.node.create': NodeCreationEvent,
            'graph.edge.create': EdgeCreationEvent,
            'graph.node.delete': NodeDeletionEvent,
            'graph.edge.delete': EdgeDeletionEvent,
            'graph.edge.update': EdgeUpdateEvent
        }
        decode_key = '.'.join(routing_key.split('.')[-3:])
        event = decode_map[decode_key]()
        event.data = json.loads(msg_body)
        return event
