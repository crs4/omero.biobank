# BEGIN_COPYRIGHT
# END_COPYRIGHT

import logging

from pygraph.classes.graph import graph
from pygraph.algorithms.searching import depth_first_search


class DependencyTree(object):
  """
  FIXME This is NOT a scalable solution. And it is hardwired to omero.
  """
  def __init__(self, kb, logger=logging.getLogger()):
    obj_klasses = [kb.Individual, kb.Vessel, kb.DataSample, kb.VLCollection,
                   kb.DataCollectionItem]
    relationship = {kb.DataCollectionItem: 'dataSample'}
    def base_ome_class(klass):
      "FIXME hardwired to Omero"
      kbase = klass.__bases__[0]
      if not kbase.OME_TABLE:
        return klass.OME_TABLE
      else:
        return base_ome_class(kbase)

    def okey(o):
      return (base_ome_class(o.__class__), o.omero_id)
    self.kb = kb
    self.logger = logger
    self.logger.info('start pre-fetching graph data')
    self.logger.info('-- start pre-fetching action data')
    actions = kb.get_objects(self.kb.Action)
    self.logger.debug('-- fetched %d actions' % len(actions))
    action_by_oid = {}
    for a in actions:
      assert a.omero_id not in action_by_oid
      action_by_oid[a.omero_id] = a
    self.logger.info('-- done pre-fetching action data')
    objs = []
    self.logger.info('-- start pre-fetching objs data')
    for k in obj_klasses:
      old_len = len(objs)
      objs.extend(kb.get_objects(k))
      self.logger.info('-- -- done pre-fetching %s' % k)
      self.logger.debug('-- -- fetched %d objects' % (len(objs) - old_len))
    self.logger.info('-- done pre-fetching objs data')
    nodes, edges = [], []
    obj_by_oid = {}
    obj_by_id = {}
    for o in objs:
      obj_by_id[o.id] = o
      obj_by_oid[okey(o)] = o
    self.logger.info('done mapping objs nodes')
    action_oid_to_object = {}
    for a in actions:
      # there could be dangling actions
      if hasattr(a, 'target') and a.target.omero_id in obj_by_oid:
        k = a.omero_id
        action_oid_to_object[k] = obj_by_oid[okey(a.target)]
    for o in objs:
      nodes.append(o.id)
      try:
        a = action_by_oid[o.action.omero_id]
        if hasattr(a, 'target'):
          x, y = o, obj_by_oid[okey(a.target)]
          if type(y) in relationship:
            y = getattr(y, relationship[type(y)])
          edges.append((x.id, y.id))
      except AttributeError, ae:
        #self.logger.debug(ae)
        pass
    gr = graph()
    gr.add_nodes(nodes)
    for e in edges:
      gr.add_edge(e)
    self.gr = gr
    self.obj_by_id = obj_by_id
    self.logger.info('done pre-fetching graph data')

  def get_connected(self, obj, aklass=None):
    st, pre, post = depth_first_search(self.gr, root=obj.id)
    if aklass:
      return [self.obj_by_id[k] for k in st.keys()
              if self.obj_by_id[k].__class__ == aklass]
    else:
      return [self.obj_by_id[k] for k in st.keys()]
