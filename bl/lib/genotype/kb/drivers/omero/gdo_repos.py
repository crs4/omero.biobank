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
    return vid



