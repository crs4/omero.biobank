from pygraph.classes.graph import graph
from pygraph.algorithms.searching import depth_first_search
from pygraph.algorithms.searching import breadth_first_search

import logging

class DependencyTree(object):
  """
  FIXME

  This is NOT a scalable solution.
  And it is hardwired to Omero.

  """
  def __init__(self, kb, logger=logging.getLogger()):
    def base_ome_class(klass):
      "FIXME hardwired to Omero"
      kbase = klass.__bases__[0]
      if not kbase.OME_TABLE:
        return klass.OME_TABLE
      else:
        return base_ome_class(kbase)

    def okey(o):
      return (base_ome_class(o.__class__), o.omero_id)
    #-
    self.kb = kb
    self.logger = logger
    self.logger.info('start pre-fetching graph data')
    #--------------------------------------------------------
    actions = kb.get_objects(self.kb.Action)
    targets = [a.target for a in actions if hasattr(a, 'target')]
    tklasses = set([t.__class__ for t in targets if t is not None])
    #--
    # DEBUG
    jf = open('junk.txt', 'w')
    #--
    action_by_oid = {}
    for a in actions:
      assert a.omero_id not in action_by_oid
      action_by_oid[a.omero_id] = a
      jf.write('action (%s, %s)\n' % (a.__class__, a.omero_id))
    self.logger.info('-- done pre-fetching action data')
    #--
    objs = []
    for k in tklasses:
      objs.extend(kb.get_objects(k))
      self.logger.info('-- done pre-fetching %s' % k)
    self.logger.info('done pre-fetching objs data')
    #--
    #--
    nodes, edges = [], []
    obj_by_oid = {}
    obj_by_id = {}
    for o in objs:
      obj_by_id[o.id] = o
      obj_by_oid[okey(o)] = o
      jf.write('object (%s, %s)\n' % (okey(o), o.id))
    #-
    self.logger.info('done mapping objs nodes')

    action_oid_to_object = {}
    for a in actions:
      # there could be dangling actions...
      if hasattr(a, 'target') and a.target.omero_id in obj_by_oid:
        k = a.omero_id
        action_oid_to_object[k] = obj_by_oid[okey(a.target)]
        jf.write('action to object (%s, %s)\n' % (k, okey(a.target)))

    for o in objs:
      nodes.append(o.id)
      a = action_by_oid[o.action.omero_id]
      if hasattr(a, 'target'):
        jf.write('re-fetching okey(a.target): %s\n' % (okey(a.target),))
        jf.flush()
        x, y = o, obj_by_oid[okey(a.target)]
        xlabel = 'NOLABEL' if not hasattr(x, 'label') else x.label
        ylabel = 'NOLABEL' if not hasattr(y, 'label') else y.label
        jf.write('%s -> %s\n' % (xlabel, ylabel))
        edges.append((x.id, y.id))
    jf.close()
    gr = graph()
    gr.add_nodes(nodes)
    for e in edges:
      gr.add_edge(e)
    self.gr = gr
    self.obj_by_id = obj_by_id
    #----------------------------------------------------------------
    self.logger.info('done pre-fetching graph data')

  def get_connected(self, obj, aklass=None):
    st, pre, post = depth_first_search(self.gr, root=obj.id)
    if aklass:
      return [self.obj_by_id[k] for k in st.keys()
              if self.obj_by_id[k].__class__ == aklass]
    else:
      return [self.obj_by_id[k] for k in st.keys()]
