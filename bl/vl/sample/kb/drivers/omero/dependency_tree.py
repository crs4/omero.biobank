from pygraph.classes.graph import graph
from pygraph.algorithms.searching import depth_first_search
from pygraph.algorithms.searching import breadth_first_search

import logging


class DependencyTree(object):
  def __init__(self, skb, ikb, klasses):
    "FIXME this separate kb handling is a major mistake."
    self.skb = skb
    self.ikb = ikb
    self.klasses = klasses
    self.logger = logging.getLogger()

    self.logger.info('start pre-fetching graph data')
    #----------------------------------------------------------------
    objs = []
    for k in klasses:
      self.logger.info('-- start pre-fetching %s' % k.get_ome_table())
      q = "select o from %s as o join fetch o.action as a" % k.get_ome_table()
      objs.extend(self.skb.find_all_by_query(q, {}, self.__factory))
      self.logger.info('-- done pre-fetching %s' % k.get_ome_table())

    action_types = set()
    for o in objs:
      if hasattr(o.action, 'target'):
        action_types.add(self.__factory(o.action.ome_obj, proxy=None).get_ome_table())

    action_to_object = {}
    for at in action_types:
      q = "select a from %s as a join fetch a.target as t" % at
      action_object = self.skb.find_all_by_query(q, {}, self.__factory)
      for a in action_object:
        action_to_object[a.id] = a.target.id
    #----------------------------------------------------------------
    self.logger.info('done pre-fetching graph data')

    otypes, nodes, edges = {}, [], []
    obj_by_id = {}
    for o in objs:
      nodes.append(o.id)
      obj_by_id[o.id] = o
      if hasattr(o.action, 'target'):
        edges.append((o.id, action_to_object[o.action.id]))

    self.obj_by_id = obj_by_id

    gr = graph()
    gr.add_nodes(nodes)
    for e in edges:
      gr.add_edge(e)
    self.gr = gr

  def get_connected(self, obj, aklass=None):
    st, pre, post = depth_first_search(self.gr, root=obj.id)
    if aklass:
      return [self.obj_by_id[k] for k in st.keys()
              if self.obj_by_id[k].__class__ == aklass]
    else:
      return [self.obj_by_id[k] for k in st.keys()]

  def __factory(self, x, proxy):
    klass_name = x.__class__.__name__[:-1] # drop the final I
    if hasattr(self.skb, klass_name):
      klass = getattr(self.skb, klass_name)
    elif hasattr(self.ikb, klass_name):
      klass = getattr(self.ikb, klass_name)
    else:
      raise ValueError('class %s is unknown' % klass_name)
    return klass(x, proxy=proxy)
