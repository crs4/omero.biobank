import vl.lib.utils as vlu

import itertools as it
import numpy     as np
import logging
import time

import table_ops

SNP_DEFINITION_TABLE = 'snp_definition.h5'
SNP_ALIGNMENT_TABLE  = 'snp_alignment.h5'
SNP_SET_DEF_TABLE    = 'snp_set_def.h5'
SNP_SET_TABLE        = 'snp_set.h5'

class Markers(object):
  """
  This acts as an entry point for all marker related operations.

  It expects that some other (sentient?) entity had generated the
  following tables:

   #. SNP_DEFINITION_TABLE[%s]
   #. SNP_ALIGNMENT_TABLE[%s]
   #. SNP_SET_TABLE[%s]

  As far as this module code is concerned, the only constraint on the
  underlying omero tables is that SNP_DEFINITION_TABLE should have a
  ``vid`` column.

  """ % (SNP_DEFINITION_TABLE, SNP_ALIGNMENT_TABLE, SNP_SET_TABLE)

  def __init__(self, proxy):
    self.proxy = proxy
    self.logger = logging.getLogger('omero_kb::Markers')
    self.logger.debug('created.')

  #----------------------
  # FIXME: It could make sense to push the op_vid support directly here.
  def __extend_snp_table(self, table_name, batch_loader, records_stream, batch_size=10000):
    self.logger.info('start extending %s' % (table_name))
    s = self.proxy.connect()
    try:
      t = table_ops.get_table(s, table_name, self.logger)
      col_objs = t.getHeaders()
      batch = batch_loader(records_stream, col_objs, batch_size)
      while batch:
        t.addData(batch)
        self.logger.debug('%s: added a batch. Current size:%d, Current time: %f' % (table_name,
                                                                                    t.getNumberOfRows(),
                                                                                    time.clock()))
        batch = batch_loader(records_stream, col_objs, batch_size)
      self.logger.info('done with extending %s: current size:%d' % (table_name, t.getNumberOfRows()))
    finally:
      self.proxy.disconnect()

  def __load_batch(self, records_stream, col_objs, chunk_size):
    """
    FIXME: Validation checks? We don't need no stinking validation checks.
    """
    v = {}
    for r in it.islice(records_stream, chunk_size):
      for k in r.keys():
        v.setdefault(k,[]).append(r[k])
    if len(v) == 0:
      return None
    for o in col_objs:
      o.values = v[o.name]
    return col_objs

  #---------------
  def extend_snp_definition_table(self, records_stream, op_vid, batch_size=50000):
    """
    TBD
    """
    vids = []
    def add_vid_filter_and_op_vid(stream, op_vid):
      for x in stream:
        x['vid'] = vlu.make_vid()
        x['op_vid'] = op_vid
        vids.append(x['vid'])
        yield x
    i_s = add_vid_filter_and_op_vid(records_stream, op_vid)
    self.__extend_snp_table(SNP_DEFINITION_TABLE, self.__load_batch, i_s, batch_size)
    return vids

  #--
  def extend_snp_alignment_table(self, records_stream, op_vid, batch_size=50000):
    def add_op_vid(stream):
      for x in stream:
        x['op_vid'] = op_vid
        yield x
    i_s = add_op_vid(records_stream)
    self.__extend_snp_table(SNP_ALIGNMENT_TABLE, self.__load_batch,
                            i_s, batch_size)

  def extend_snp_set_def_table(self, maker, model, op_vid):
    set_vid = vlu.make_vid()
    def stream():
      for x in [{'vid':set_vid, 'maker': maker, 'model' : model, 'op_vid':op_vid}]:
        yield x
    i_s = stream()
    self.__extend_snp_table(SNP_SET_DEF_TABLE, self.__load_batch, i_s)
    return set_vid

  def extend_snp_set_table(self, set_vid, records_stream, op_vid, batch_size=50000):
    def add_op_vid(stream):
      for x in stream:
        x['vid'] = set_vid
        x['op_vid'] = op_vid
        yield x
    i_s = add_op_vid(records_stream)
    self.__extend_snp_table(SNP_SET_TABLE, self.__load_batch,
                            i_s, batch_size)
    return set_vid
  #---------------

  def __get_snp_table_rows_selected(self, table, selector, batch_size):
    res, row_read, max_row = [], 0, table.getNumberOfRows()
    self.logger.debug('selecting on: %s across %d records' % (selector, max_row))
    while row_read < max_row:
      ids = table.getWhereList(selector, {}, row_read, row_read + batch_size, 1)
      self.logger.debug('selecting on: %s [%d:%d:%d] returned %d records' % (selector,
                                                                             row_read,
                                                                             row_read+batch_size,
                                                                             1,
                                                                             len(ids)))
      if ids:
        d = table.readCoordinates(ids)
        res.append(table_ops.convert_to_np(d))
      row_read += batch_size
    return np.concatenate(tuple(res)) if res else []

  def __get_snp_table_rows_bulk(self, table, batch_size):
    res, row_read, max_row = [], 0, table.getNumberOfRows()
    self.logger.debug('bulk reading across %d records' % max_row)
    col_objs = table.getHeaders()
    while row_read < max_row:
      d = table.read(range(len(col_objs)), row_read, row_read + batch_size)
      self.logger.debug('bulk reading [%d:%d] returned %d records' % (row_read, row_read+batch_size,
                                                                      len(d.columns[0].values)))
      if d:
        res.append(table_ops.convert_to_np(d))
      row_read += batch_size
    return np.concatenate(tuple(res)) if res else []

  def __get_snp_table_rows(self, table_name, selector, batch_size):
    self.logger.info('start get from table %s: selector:%s' % (table_name, selector))
    s = self.proxy.connect()
    t = table_ops.get_table(s, table_name, self.logger)
    if selector:
      res = self.__get_snp_table_rows_selected(t, selector, batch_size)
    else:
      res = self.__get_snp_table_rows_bulk(t, batch_size)
    self.proxy.disconnect()
    self.logger.info('done with get from table %s: no. rows extracted:%d' % (table_name, len(res)))
    return res

  def get_snp_definition_table_rows(self, selector=None, batch_size=50000):
    """
    selector = "(source == 'affymetrix') & (context == 'GW6.0')"
    """
    return self.__get_snp_table_rows(SNP_DEFINITION_TABLE, selector, batch_size)

  def get_snp_alignment_table_rows(self, selector=None, batch_size=50000):
    """
    selector = "( ref_genome == 'hg18') & (global_pos >  2909900) & (global_pos < 298129829)"
    """
    return self.__get_snp_table_rows(SNP_ALIGNMENT_TABLE, selector, batch_size)

  def get_snp_set_def_table_rows(self, selector=None, batch_size=50000):
    """
    selector = "()"
    """
    return self.__get_snp_table_rows(SNP_SET_DEF_TABLE, selector, batch_size)

  def get_snp_set_table_rows(self, selector=None, batch_size=50000):
    """
    selector = "()"
    """
    return self.__get_snp_table_rows(SNP_SET_TABLE, selector, batch_size)


  def select_by_coords(self, pos_min, pos_max, build=None):
    """
    ::
    build='hg19'
    mrks = kb.Markers()
    pos_min = (10, 290090)
    pos_max = (10, 291090)
    vid_array = mrks.select_by_coords(pos_min, pos_max, build=build)
    """
    pass

  def get_markers(self, vid_array):
    """
    mrk_array = mrks.get_markers(vid_array)
    for (v, m) in it.zip(vid_array, mrk_array):
      self.assertTrue(m['vid'] == v and m['pos'] >= pos_min and m['pos'] < pos_max)
    """
    pass

  def create_markers_array(self, N):
    """
    TBD
    """
    X = np.zeros(N, dtype={'rslabel':('S16', 0, 'rs label'),
                           'chr'    :('i2',  1, 'chromosome'),
                           'pos'    :('i8',  2, 'position in chromosome'),
                           'build'  :('i4',  3, 'build numeric id')})
    return X


  def save_markers(self, mrk_array):
    """
    TBD
    """
    pass
