import json
from bl.vl.kb.drivers.omero.utils import ome_hash


class BasicEvent(object):

    def __init__(self, event_type, data=None):
        self.event_type = event_type
        self.data = data

    @property
    def json_data(self):
        return json.dumps(self.data)


class NodeCreationEvent(BasicEvent):

    def __init__(self, bl_obj):
        super(NodeCreationEvent, self).__init__('graph.node.create')
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

    def __init__(self, bl_act, bl_src_obj, bl_dest_obj):
        super(EdgeCreationEvent, self).__init__('graph.edge.create')
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

    def __init__(self, bl_obj):
        super(NodeDeletionEvent, self).__init__('graph.node.delete')
        data = {
            'action': 'NODE_DELETE',
            'target': ome_hash(bl_obj.ome_obj)
        }
        self.data = data


class EdgeDeletionEvent(BasicEvent):

    def __init__(self, bl_act):
        super(EdgeDeletionEvent, self).__init__('graph.edge.delete')
        data = {
            'action': 'EDGE_DELETE',
            'target': ome_hash(bl_act.ome_obj)
        }
        self.data = data


class EdgeUpdateEvent(BasicEvent):

    def __init__(self, bl_act, bl_src_obj=None, bl_dest_obj=None):
        super(EdgeUpdateEvent, self).__init__('graph.edge.update')
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