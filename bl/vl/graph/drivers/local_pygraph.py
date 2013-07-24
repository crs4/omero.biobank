# BEGIN_COPYRIGHT
# END_COPYRIGHT

from bl.vl.utils import get_logger
from pygraph.classes.graph import graph
from pygraph.algorithms.searching import depth_first_search


class PygraphDriver(object):

    BLOCKED_PROXY_CALLBACKS = ()
    """
    FIXME This is NOT a scalable solution. And it is hardwired to omero.
    """
    def __init__(self, kb, logger=get_logger('local-pygraph-driver')):
        self.kb = kb
        if self.kb.logger:
            self.logger = self.kb.logger
        else:
            self.logger = logger
        self.obj_klasses = [
            kb.Individual,
            kb.Vessel,
            kb.DataSample,
            kb.VLCollection,
            kb.DataCollectionItem,
            kb.LaneSlot,
        ]
        self.relationship = {
            kb.DataCollectionItem: 'dataSample',
            kb.VesselsCollectionItem: 'vessel',
        }
        self.graph = None
        self.objects_map_by_id = None

    def __base_ome_class__(self, klass):
        kbase = klass.__bases__[0]
        if not kbase.OME_TABLE:
            return klass.OME_TABLE
        else:
            return self.__base_ome_class__(kbase)

    def __okey__(self, o):
        return self.__base_ome_class__(o.__class__), o.omero_id

    def setup(self):
        self.logger.info('start pre-fetching graph data')
        self.logger.info('-- start pre-fetching action data')
        actions = self.kb.get_objects(self.kb.Action)
        self.logger.debug('-- fetched %d actions' % len(actions))
        action_by_oid = {}
        for a in actions:
            assert a.omero_id not in action_by_oid
            action_by_oid[a.omero_id] = a
        self.logger.info('-- done pre-fetching action data')
        objs = []
        self.logger.info('-- start pre-fetching objs data')
        for k in self.obj_klasses:
            old_len = len(objs)
            objs.extend(self.kb.get_objects(k))
            self.logger.info('-- -- done pre-fetching %s and subclasses' % k)
            self.logger.debug('-- -- fetched %d objects' % (len(objs) - old_len))
        self.logger.info('-- done pre-fetching objs data')
        nodes, edges = [], []
        obj_by_oid = {}
        obj_by_id = {}
        for o in objs:
            obj_by_id[o.id] = o
            obj_by_oid[self.__okey__(o)] = o
        self.logger.info('done mapping objs nodes')
        action_oid_to_object = {}
        for a in actions:
            # there could be dangling actions
            if hasattr(a, 'target') and a.target.omero_id in obj_by_oid:
                k = a.omero_id
                action_oid_to_object[k] = obj_by_oid[self.__okey__(a.target)]
        for o in objs:
            nodes.append(o.id)
            try:
                a = action_by_oid[o.action.omero_id]
                if hasattr(a, 'target'):
                    x, y = o, obj_by_oid[self.__okey__(a.target)]
                    if type(y) in self.relationship:
                        y = getattr(y, self.relationship[type(y)])
                    edges.append((x.id, y.id))
            except AttributeError:
                pass
        gr = graph()
        gr.add_nodes(nodes)
        for e in edges:
            gr.add_edge(e)
        self.graph = gr
        self.objects_map_by_id = obj_by_id
        self.logger.info('done pre-fetching graph data')

    def get_connected(self, obj, aklass=None):
        if self.graph is None:
            self.setup()
        st, pre, post = depth_first_search(self.graph, root=obj.id)
        if aklass:
            return [self.objects_map_by_id[k] for k in st.keys()
                    if self.objects_map_by_id[k].__class__ == aklass]
        else:
            return [self.objects_map_by_id[k] for k in st.keys()]

    def create_node(self, obj):
        pass

    def create_edge(self, act, source, dest):
        pass

    def destroy_node(self, obj):
        pass

    def destroy_edge(self, src, dest):
        pass

    def destroy_edges(self, act):
        pass

    def modify_edge(self, act, source, dest):
        pass