import os, unittest, time
import itertools as it
import numpy as np
from bl.vl.kb import KBError

from bl.vl.kb.drivers.omero.proxy_core import ProxyCore

OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")

class TestProxyCore(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def make_fields(self):
    fields =  [('string', 'r_type', 'Result object type',       256, None),
               ('string', 'r_vid',  'Result object VID',        64, None),
               ('long',   'r_id',   'Result object ID',         None),
               ('string', 'o_vid',  'Action VID',               64, None),
               ('string', 't_vid',  'Action target object VID', 64, None),
               ('string', 'i_vid',  'Root tree VID',            64, None)]
    return fields

  def test_create_delete(self):
    fields = self.make_fields()
    table_name = 'foo-%s.h5' % time.time()

    pc = ProxyCore(OME_HOST, OME_USER, OME_PASS)
    pc.create_table(table_name, fields)
    rec_desc = pc.get_table_headers(table_name)
    for f, n in it.izip(fields, rec_desc):
      self.assertEqual(f[1], n[0])
      if f[0] == 'string':
        self.assertEqual('|S%d' % f[3], n[1])
      elif f[0] == 'long':
        self.assertEqual('i8', n[1])
    pc.delete_table(table_name)

  def fill_table(self, pc, table_name, N):
    rec_type = pc.get_table_headers(table_name)
    data = np.zeros(N, dtype=rec_type)
    data['r_id'] =  np.arange(N)
    data['r_type'] = ['r_type%04d' % i for i in range(N)]
    data['r_vid'] = ['r_vid%04d' % i for i in range(N)]
    data['o_vid'] = ['o_vid%04d' % i for i in range(N)]
    data['t_vid'] = ['t_vid%04d' % i for i in range(N)]
    data['i_vid'] = ['i_vid%04d' % i for i in range(N)]
    pc.add_table_rows(table_name, data[:-1])
    pc.add_table_row(table_name, data[-1])
    return data

  def test_get_table_rows(self):
    fields = self.make_fields()
    table_name = 'foo-%s.h5' % time.time()
    N = 16
    pc = ProxyCore(OME_HOST, OME_USER, OME_PASS)
    pc.create_table(table_name, fields)
    data = self.fill_table(pc, table_name, N)
    # -- selector classic
    irange = range(N/4, (3*N)/4)
    selectors = ['(r_vid == "%s")' % data[i]['r_vid'] for i in irange]
    #--
    sel_classic = '|'.join(selectors)
    rows_classic = pc.get_table_rows(table_name, selector=sel_classic)
    for i, r in it.izip(irange, rows_classic):
      self.assertTrue(data[i] == r)
    #--
    sel_lite    = selectors
    rows_lite = pc.get_table_rows(table_name, selector=sel_lite)
    for i, r in it.izip(irange, rows_classic):
      self.assertTrue(data[i] == r)

  def test_update_row(self):
    fields = self.make_fields()
    table_name = 'foo-%s.h5' % time.time()
    N = 16
    #--
    pc = ProxyCore(OME_HOST, OME_USER, OME_PASS)
    pc.create_table(table_name, fields)
    data = self.fill_table(pc, table_name, N)
    r = pc.get_table_rows(table_name, None)
    self.assertTrue(np.all(data == r))
    #--
    m = N/2
    r = pc.get_table_rows(table_name, '(r_vid == "%s")' % data[m]['r_vid'])
    self.assertEqual(len(r), 1)
    self.assertTrue(data[m] == r[0])
    urow = data[m].copy()
    urow['o_vid'] = 'foo' + urow['o_vid']
    #-
    r = pc.get_table_rows(table_name, '(r_vid == "%s")' % data[m]['r_vid'])
    self.assertEqual(len(r), 1)
    pc.update_table_row(table_name, '(r_vid == "%s")' % data[m]['r_vid'], urow)
    #
    r = pc.get_table_rows(table_name, '(r_vid == "%s")' % data[m]['r_vid'])
    self.assertEqual(len(r), 1)
    self.assertTrue(urow == r[0])
    #
    pc.delete_table(table_name)

  def test_table_rows(self):
    fields = self.make_fields()
    table_name = 'foo-%s.h5' % time.time()
    N = 16
    #--
    pc = ProxyCore(OME_HOST, OME_USER, OME_PASS)
    pc.create_table(table_name, fields)
    data = self.fill_table(pc, table_name, N)
    r = pc.get_table_rows(table_name, None)
    self.assertTrue(np.all(data == r))
    #--
    m = N/2
    r = pc.get_table_rows(table_name, '(r_vid == "%s")' % data[m]['r_vid'])
    self.assertEqual(len(r), 1)
    self.assertTrue(data[m] == r[0])
    pc.delete_table(table_name)

  def test_table_rows_iterator(self):
    fields = self.make_fields()
    table_name = 'foo-%s.h5' % time.time()
    N = 16
    #--
    pc = ProxyCore(OME_HOST, OME_USER, OME_PASS)
    pc.create_table(table_name, fields)
    data = self.fill_table(pc, table_name, N)
    row_it = pc.get_table_rows_iterator(table_name)
    for i, row in enumerate(row_it):
      self.assertTrue(row == data[i])
    pc.delete_table(table_name)

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestProxyCore('test_create_delete'))
  suite.addTest(TestProxyCore('test_table_rows'))
  suite.addTest(TestProxyCore('test_table_rows_iterator'))
  suite.addTest(TestProxyCore('test_get_table_rows'))
  suite.addTest(TestProxyCore('test_update_row'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
