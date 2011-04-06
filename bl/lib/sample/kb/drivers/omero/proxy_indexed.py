from proxy_core import ProxyCore

import vl.lib.utils     as vlu
import numpy            as np


class ProxyIndexed(ProxyCore):

  ACTION_TABLE='vl_action_table.h5'
  ACTION_RESULT_CLASS_MAX_NAME_LEN = 256
  ACTION_TABLE_COLUMNS =  [('string', 'r_type', 'Result object type', ACTION_RESULT_CLASS_MAX_NAME_LEN, None),
                           ('string', 'r_vid',  'Result object VID',        len(vlu.make_vid()), None),
                           ('long',   'r_id',   'Result object ID',         None),
                           ('string', 'o_vid',  'Action VID',               len(vlu.make_vid()), None),
                           ('string', 't_vid',  'Action target object VID', len(vlu.make_vid()), None),
                           ('string', 'i_vid',  'Root tree VID',            len(vlu.make_vid()), None)]
  ROOT_VID = 'VROOT'

  ACTION_INDEXED_TYPES = []

  def __init__(self, host, user, passwd):
    super(ProxyIndexed, self).__init__(host, user, passwd)
    self.create_if_missing(self.ACTION_TABLE, self.ACTION_TABLE_COLUMNS)

  #---------------------------------------------------------------------------------------------
  def save(self, obj):
    obj = super(ProxyIndexed, self).save(obj)
    if filter(lambda x: isinstance(obj, x), self.ACTION_INDEXED_TYPES):
      r_vid = obj.vid
      t_vid = obj.action.target.vid._val if hasattr(obj.action, 'target') else ''
      if t_vid:
        row = self.get_table_rows(self.ACTION_TABLE, selector='(r_vid == "%s")' % t_vid)
        if row:
          i_vid = row[0]['i_vid']
        else:
          i_vid = t_vid
      else:
        i_vid = r_vid
      #-
      row = {'r_type' : type(obj).__name__, 'r_vid' : r_vid, 'r_id'  : obj.ome_obj.id._val,
             'o_vid' :obj.action.vid, 't_vid' : t_vid, 'i_vid' : i_vid}
      print 'row:', row
      self.add_table_row(self.ACTION_TABLE, row)
    return obj

  def create_if_missing(self, table_name, fields):
    if not self.table_exists(table_name):
      self.create_table(table_name, fields)
      row = {'r_type' : None, 'r_vid' : self.ROOT_VID, 'r_id' : 0, 'o_vid' : None, 't_vid': None, 'i_vid': None}
      self.add_table_row(self.ACTION_TABLE, row)

  def get_objects_linked_to_object(self, object):
    data = self.get_table_rows(self.ACTION_TABLE, selector, batch_size)
    #[apply(Foo, ome_obj) for Foo, ome_obj in data]

  def get_actions_tree(self, vid):
    pass
  #----------------------------------------------------------------------------------------------


