import vl.lib.utils as vlu

import itertools as it
import numpy     as np
import logging
import time
import table_ops

class GdoRepos(object):
  """
  This acts as an entry point for all SNP Genotype Data Object
  Repositories operations.

  """
  def __init__(self, proxy):
    self.proxy = proxy
    self.logger = logging.getLogger('omero_kb::GdoRepos')
    self.logger.debug('created.')
    self.index = {}

  def table_name(self, set_vid):
    return '%s.h5' % set_vid

  def create_repository(self, set_vid, N):
    table_name = self.table_name(set_vid)
    self.logger.info('start creating %s %s' % (table_name, N))
    s = self.proxy.connect()
    table_ops.create_snp_gdo_repository_table(s, table_name, N, self.logger)
    self.proxy.disconnect()
    self.logger.info('done creating %s %s' % (table_name, N))
    return set_vid

  def append(self, set_vid, probs, confidence, op_vid):
    table_name = self.table_name(set_vid)
    vid = vlu.make_vid()
    self.logger.info('start appending %s to %s [%s]' % (vid, set_vid, op_vid))
    s = self.proxy.connect()
    t = table_ops.get_table(s, table_name, self.logger)
    col_objs = t.getHeaders()
    # FIXME: this is dangerous, it assumes that we know details
    #        on the table implementation...
    pstr = probs.tostring()
    cstr = confidence.tostring()
    assert len(pstr) == 2*len(cstr)
    assert col_objs[2].size == len(pstr)
    assert col_objs[3].size == len(cstr)
    col_objs[0].values = [vid]
    col_objs[1].values = [op_vid]
    col_objs[2].values = [pstr]
    col_objs[3].values = [cstr]
    t.addData(col_objs)
    self.proxy.disconnect()
    self.logger.info('done appending')
    try:
      del self.index[table_name]
    except KeyError, e:
      pass
    return vid

  def __cache_indices(self, t, table_name):
    if not self.index.has_key(table_name):
      v = t.read([0], 0, t.getNumberOfRows())
      self.index[table_name] = dict(it.izip(v.columns[0].values, v.rowNumbers))

  def __unwrap_gdo(self, set_id, v, k):
    row_id = v.rowNumbers[k]
    vid =  v.columns[0].values[k]
    op_vid = v.columns[1].values[k]
    #--
    probs, confs = v.columns[2].values[k], v.columns[3].values[k]
    #--
    probs = probs + chr(0) * (v.columns[2].size - len(probs))
    probs = np.fromstring(probs, dtype=np.float32)
    probs.shape = (2, probs.shape[0]/2)
    #--
    confs = confs + chr(0) * (v.columns[3].size - len(confs))
    confs = np.fromstring(confs, dtype=np.float32)
    #--
    self.logger.info('unwrapping [%d]->%s' % (row_id, vid))
    return {'set_id': set_id, 'row_id' : row_id, 'vid' : vid,
            'probs'  : probs,  'confs' : confs,
            'op_vid' : op_vid}

  def get(self, set_vid, vid):
    table_name = self.table_name(set_vid)
    self.logger.info('start get %s from %s' % (vid, set_vid))
    s = self.proxy.connect()
    #--
    t = table_ops.get_table(s, table_name, self.logger)
    self.__cache_indices(t, table_name)
    row_id = self.index[table_name][vid]
    v = t.read(range(0,4), row_id, row_id+1)
    r = self.__unwrap_gdo(set_id, v, 0)
    assert r['row_id'] == row_id and r['vid'] == vid and r['set_id'] == set_id
    #--
    self.proxy.disconnect()
    self.logger.info('done get %s from %s' % (vid, set_vid))
    return r

  def get_gdo_stream(self, set_vid, batch_size=10):
    """FIXME error control..."""
    #-
    def iter_on_gdo(t):
      i, N = 0, t.getNumberOfRows()
      self.logger.info('start get_gdo_stream.iter_on_gdo %s[%d]' % (set_vid, N))
      while i < N:
        j = min(N, i + batch_size)
        v = t.read(range(0,4), i, j)
        for k in range(j - i):
          yield self.__unwrap_gdo(v, k)
        i = j
      self.logger.info('done get_gdo_stream %s' % (set_vid))
      # this closes the connect() below
      self.proxy.disconnect()
    #-
    table_name = self.table_name(set_vid)
    self.logger.info('start get_gdo_stream on %s' % (set_vid))
    s = self.proxy.connect()
    #--
    t = table_ops.get_table(s, table_name, self.logger)
    return iter_on_gdo(t)



