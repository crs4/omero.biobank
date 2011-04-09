from proxy_core import ProxyCore
from proxy_core import debug_boundary


import bl.vl.utils     as vlu
import numpy           as np

import logging

logger = logging.getLogger()

class ProxyIndexed(ProxyCore):

  ACTION_TABLE='vl_action_table_v0.h5'
  ACTION_RESULT_CLASS_MAX_NAME_LEN = 256
  ACTION_TABLE_COLUMNS =  [('string', 'r_type', 'Omero table name', ACTION_RESULT_CLASS_MAX_NAME_LEN, None),
                           ('string', 'r_vl_class', 'Virgil class name', ACTION_RESULT_CLASS_MAX_NAME_LEN, None),
                           ('string', 'r_vl_module', 'Virgil class module', ACTION_RESULT_CLASS_MAX_NAME_LEN, None),
                           ('string', 'r_vid',  'Result object VID',        len(vlu.make_vid()), None),
                           ('long',   'r_id',   'Result object ID',         None),
                           ('long',   'o_id',   'Action VID',               None),
                           ('long',   't_id',   'Action target object ID',  None),
                           ('long',   'i_id',   'Root tree ID',             None)]
  ROOT_VID = 'VROOT'

  ACTION_INDEXED_TYPES = []

  def __init__(self, host, user, passwd):
    super(ProxyIndexed, self).__init__(host, user, passwd)
    self.create_if_missing(self.ACTION_TABLE, self.ACTION_TABLE_COLUMNS)

  #---------------------------------------------------------------------------------------------
  @debug_boundary
  def create_if_missing(self, table_name, fields):
    if not self.table_exists(table_name):
      self.create_table(table_name, fields)
      # FIXME we need to do this because otherwise a table.getNumberOfRows() in get_table_rows will fail.
      #       this appears to be an upstream bug in the omero table support.
      row = {'r_type' : None, 'r_vl_class' : None, 'r_vl_module' : None,
             'r_vid' : self.ROOT_VID, 'r_id' : 0, 'o_id' : 0, 't_id': 0, 'i_id': 0}
      self.add_table_row(self.ACTION_TABLE, row)

  @debug_boundary
  def _fetch_ome_object(self, obj):
    if not obj.ome_obj.loaded:
      query = 'select a from %s a where a.id = :a_id' % obj.OME_TABLE
      pars = self.ome_query_params({'a_id' : obj.ome_obj._id})
      return self.ome_operation("getQueryService", "findByQuery", query, pars)
      #action_obj = self.ome_operation("getQueryService", "get", "Action", action_obj._id._val)
    else:
      return obj.ome_obj

  @debug_boundary
  def save(self, obj):
    obj = super(ProxyIndexed, self).save(obj)
    if filter(lambda x: isinstance(obj, x), self.ACTION_INDEXED_TYPES):
      #FIXME this is silly, it should be possible to do o.id and o.vid
      r_vid = obj.vid
      r_id  = obj.ome_obj.id._val
      #-
      action_obj = self._fetch_ome_object(obj.action)
      o_id = action_obj._id._val
      t_id = action_obj.target.id._val if hasattr(action_obj, 'target') else 0
      if t_id:
        row = self.get_table_rows(self.ACTION_TABLE, selector='(r_id == %d)' % t_id)
        if row:
          i_id = int(row[0]['i_id'])
        else:
          i_id = t_id
      else:
        i_id = r_id
      #-
      row = {'r_type' : obj.OME_TABLE,
             'r_vl_class' : obj.__class__.__name__, 'r_vl_module' : obj.__module__,
             'r_vid' : r_vid, 'r_id'  : r_id,
             'o_id' : o_id, 't_id' : t_id, 'i_id' : i_id}
      logger.debug('new row: %s' % row)
      self.add_table_row(self.ACTION_TABLE, row)
    return obj


  @debug_boundary
  def __extract_object(self, row):
    r_id   = int(row['r_id'])
    r_type = str(row['r_type'])
    mod   = str(row['r_vl_module'])
    klass = str(row['r_vl_class'])
    ome_obj = self.ome_operation("getQueryService", "get", r_type, r_id)
    _tmp = __import__(mod, globals(), locals(), [klass], -1)
    obj = getattr(_tmp, klass)(ome_obj)
    return obj

  @debug_boundary
  def get_root(self, object):
    o_rows = self.get_table_rows(self.ACTION_TABLE, '(r_vid=="%s")' % object.id)
    assert len(o_rows) == 1
    o_row = o_rows[0]
    if o_row['i_id'] == o_row['r_id']:
      return object
    r_rows = self.get_table_rows(self.ACTION_TABLE, '(r_id==%d)' % o_row['i_id'])
    assert len(r_rows) == 1
    r_row = r_rows[0]
    return self.__extract_object(r_row)

  @debug_boundary
  def get_descendants(self, obj, klass=None):
    o_id = obj.ome_obj.id._val
    o_rows = self.get_table_rows(self.ACTION_TABLE, '(i_id==%d)' % o_id)
    if len(o_rows) == 0:
      return None
    return [self.__extract_object(r) for r in o_rows
            if not r['r_id'] == o_id and ((not klass)
                                          or  r['r_type'] == klass.OME_TABLE)]

  def get_actions_tree(self, vid):
    pass
  #----------------------------------------------------------------------------------------------


