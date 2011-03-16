import details as okbd
import vl.lib.utils as vlu

import itertools as it
import numpy     as np
import logging
import time

class GdoRepos(okbd.Proxy):
  """
  This acts as an entry point for all SNP Genotype Data Object
  Repositories operations.

  """
  def __init__(self, host, user, passwd):
    super(GdoRepos, self).__init__(host, user, passwd)
    self.logger = logging.getLogger('omero_kb::GdoRepos')
    self.logger.debug('created.')
    self.index = {}


  def table_name(self, set_vid):
    return '%s.hd5' % set_vid

  def create_repository(self, set_vid, N):
    table_name = self.table_name(set_vid)
    self.logger.info('start creating %s %s' % (table_name, N))
    s = self.connect()
    okbd.create_snp_gdo_repository_table(s, table_name, N, self.logger)
    self.disconnect()
    self.logger.info('done creating %s %s' % (table_name, N))

  def append(self, set_vid, probs, confidence, op_vid):
    table_name = self.table_name(set_vid)
    vid = vlu.make_vid()
    self.logger.info('start appending %s to %s [%s]' % (vid, set_vid, op_vid))
    s = self.connect()
    t = okbd.get_table(s, table_name, self.logger)
    col_objs = t.getHeaders()
    # FIXME: this is dangerous, it assumes that we know details
    #        on the table implementation...
    col_objs[0].values = [vid]
    col_objs[1].values = [op_vid]
    col_objs[2].values = [probs.tostring()]
    col_objs[3].values = [confidence.tostring()]
    t.addData(col_objs)
    self.disconnect()
    self.logger.info('done appending')
    try:
      del self.index[table_name]
    except KeyError, e:
      pass
    return vid

  def get(self, set_vid, vid):
    table_name = self.table_name(set_vid)
    self.logger.info('start get %s from %s' % (vid, set_vid))
    s = self.connect()
    #--
    t = okbd.get_table(s, table_name, self.logger)
    if not self.index.has_key(table_name):
      v = t.read([0], 0, t.getNumberOfRows())
      self.index[table_name] = dict(it.izip(v.columns[0].values, v.rowNumbers))
    row_id = self.index[table_name][vid]
    v = t.read(range(0,4), row_id, row_id+1)
    assert v.rowNumbers[0] == row_id
    assert v.columns[0].values[0] == vid
    #--
    probs = np.fromstring(v.columns[2].values[0], dtype=np.float32)
    probs.shape = (2, probs.shape[0]/2)
    confs = np.fromstring(v.columns[3].values[0], dtype=np.float32)
    op_vid = v.columns[3].values[0]
    assert confs.shape[0] == probs.shape[1]
    #--
    self.disconnect()
    self.logger.info('done get %s from %s' % (vid, set_vid))
    return probs, confs, op_vid



