"""

ProxyIndexed
============

The goal of this wrapper class is to speed-up object searches and the traversal of the dependencies tree.


save(obj):


  if isinstance(obj, Action):
      if hasattr(obj, target):
         (do not write it twice...)
         write record  A_VID, A_ID, T_VID, T_TABLE_NAME, T_ID

  if isinstance(obj, Result):

      ar =  action record for obj.action.id
      if ar:
         tr = fetch target record for T_VID
         write record O_VID, O_ID, O_TABLE_NAME, O_KLASS, O_MODULE, tr.root_vid
      else:
         write record O_VID, O_ID, O_TABLE_NAME, O_KLASS, O_MODULE, O_VID

FIXME: Current implementation does not handle object deletion.
"""

from proxy_core import ProxyCore
from indexer    import Indexer
from action     import Action

import bl.vl.utils     as vlu
import numpy           as np

import logging
import time

logger = logging.getLogger('proxy_indexed')

counter = 0
def debug_boundary(f):
  def debug_boundary_wrapper(*args, **kv):
    global counter
    now = time.time()
    counter += 1
    logger.debug('%s[%d] in' % (f.__name__, counter))
    res = f(*args, **kv)
    logger.debug('%s[%d] out (%f)' % (f.__name__, counter,
                                      time.time() - now))
    counter -= 1
    return res
  return debug_boundary_wrapper


class ProxyIndexed(ProxyCore):

  INDEXED_TARGET_TYPES = []

  def __init__(self, host, user, passwd, session_keep_tokens=1):
    super(ProxyIndexed, self).__init__(host, user, passwd, session_keep_tokens)
    self.indexer = Indexer(self)


  @debug_boundary
  def save(self, obj):
    logger.debug('processing %s with vid: %s' % (obj.get_ome_table(), obj.id))
    obj = super(ProxyIndexed, self).save(obj)
    if isinstance(obj, Action) and hasattr(obj, 'target'):
      self.indexer.record_action(obj)
    elif filter(lambda x: isinstance(obj, x), self.INDEXED_TARGET_TYPES):
      self.indexer.record_target(obj)
    else:
      logger.debug('no recording of %s with vid: %s' % (obj.get_ome_table(), obj.id))
    return obj

  @debug_boundary
  def delete(self, obj):
    logger.debug('processing %s with vid: %s' % (obj.get_ome_table(), obj.id))
    try:
      if isinstance(obj, Action) and hasattr(obj, 'target'):
        self.indexer.delete_action(obj)
      elif filter(lambda x: isinstance(obj, x), self.INDEXED_TARGET_TYPES):
        self.indexer.delete_target(obj)
    finally:
      obj = super(ProxyIndexed, self).delete(obj)

  @debug_boundary
  def get_root(self, object, aklass=None):
    return self.indexer.get_root(object, aklass)

  @debug_boundary
  def get_descendants(self, obj, klass=None):
    """
    FIMXE. This function will work only if obj == get_root(obj)
    """
    return self.indexer.get_descendants(obj, klass)

  def get_actions_tree(self, vid):
    pass
  #----------------------------------------------------------------------------------------------


