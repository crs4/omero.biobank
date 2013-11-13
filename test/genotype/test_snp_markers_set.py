# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, time, os, random
import itertools as it
import tempfile
import numpy as np

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.kb.drivers.omero.genomics import MSET_TABLE_COLS_DTYPE

from common import UTCommon

import bl.vl.genotype.io as gio

OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'root')
OME_PASSWD = os.getenv('OME_PASSWD', 'romeo')


class markers_set(UTCommon):

  def __init__(self, name):
    super(markers_set, self).__init__(name)
    self.kill_list = []

  def setUp(self):
    self.kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASSWD)
    conf = {
      'label': 'TEST-%f' % time.time(),
      'description': 'unit test garbage',
      }
    self.study = self.kb.factory.create(self.kb.Study, conf).save()
    self.kill_list.append(self.study)
    self.action = self.kb.create_an_action(self.study)
    self.kill_list.append(self.action)

  def tearDown(self):
    while self.kill_list:
      self.kb.delete(self.kill_list.pop())

  def test_creation_destruction(self):
    N = 32
    mset, rows = self.create_markers_set(N)
    self.kill_list.append(mset)
    markers = self.kb.genomics.get_markers_array_rows(mset)
    self.assertEqual(len(markers), N)
    for r, m in it.izip(rows, markers):
      self.assertEqual(len(r), len(m))
      for i, x in enumerate(r):
        self.assertEqual(x, m[i])

  def test_read_ssc(self):
    N = 16
    N_dups = 4
    mset, rows = self.create_markers_set(N)
    self.kill_list.append(mset)    
    probs, confs = self.make_fake_data(N)
    sample_id = 'ffoo-%f' % time.time()
    fn = tempfile.NamedTemporaryFile().name
    self.make_fake_ssc(mset, rows['label'], sample_id, probs, confs, fn)
    probs_1, confs_1 = gio.read_ssc(fn, mset)
    self.assertAlmostEqual(np.sum(np.abs(probs - probs_1)), 0.0)
    self.assertAlmostEqual(np.sum(np.abs(confs - confs_1)), 0.0)

  def test_gdo(self):
    N = 32
    mset, _ = self.create_markers_set(N)
    self.kill_list.append(mset)    
    data_sample = self.create_data_sample(mset, 'foo-data', self.action)
    self.kill_list.append(data_sample)
    data_obj, probs, confs = self.create_data_object(data_sample, self.action)
    self.kill_list.append(data_obj)    
    probs1, confs1 = data_sample.resolve_to_data()
    self.assertTrue((probs == probs1).all())
    self.assertTrue((confs == confs1).all())
    s = self.kb.genomics.get_gdo_iterator(mset, data_samples=[data_sample])
    for i, x in enumerate(s):
      self.assertTrue((probs == x['probs']).all())
      self.assertTrue((confs == x['confidence']).all())
    self.assertEqual(i, 0)
    indices = slice(N/4, N/2)
    s = self.kb.genomics.get_gdo_iterator(
      mset, data_samples=[data_sample], indices=indices
      )
    for i, x in enumerate(s):
      self.assertTrue((probs[:,indices] == x['probs']).all())
      self.assertTrue((confs[indices] == x['confidence']).all())
      self.assertEqual(i, 0)


  def test_speed(self):
    ref_genome = 'g' + ('%f' % time.time())[-14:]
    N1 = 1024*1024
    N2 = N1/2
    beg = time.time()
    print ''
    print 'creating %d markers took %f' % (N1, time.time() - beg)
    beg = time.time()
    mset1, _ = self.create_markers_set(N1)
    self.kill_list.append(mset)        
    print 'creating a markers set with %d markers took %f' % (
      N1, time.time() - beg
      )

  def test_speed_gdo(self):
    N = 934968
    beg = time.time()
    print ''
    print 'creating %d markers took %f' % (N, time.time() - beg)
    mset, _ = self.create_markers_set(N)
    self.kill_list.append(mset)            
    beg = time.time()
    data_sample = self.create_data_sample(mset, 'foo-data', self.action)
    self.kill_list.append(data_sample)    
    print 'creating a data sample took %f' % (time.time() - beg)
    beg = time.time()
    do, probs, confs = self.create_data_object(data_sample, self.action)
    self.kill_list.append(do)        
    print 'creating a data object took %f' % (time.time() - beg)
    beg = time.time()
    probs1, confs1 = data_sample.resolve_to_data()
    print 'resolving to data  took %f' % (time.time() - beg)
    self.assertTrue((probs == probs1).all())
    self.assertTrue((confs == confs1).all())
    beg = time.time()
    s = self.kb.genomics.get_gdo_iterator(mset, data_samples=[data_sample])
    for i, x in enumerate(s):
      self.assertTrue((probs == x['probs']).all())
      self.assertTrue((confs == x['confidence']).all())
    self.assertEqual(i, 0)
    print 'iterating took %f' % (time.time() - beg)
    indices = slice(N/4, N/2)
    beg = time.time()
    s = self.kb.genomics.get_gdo_iterator(
      mset, data_samples=[data_sample], indices=indices
      )
    for i, x in enumerate(s):
      self.assertTrue((probs[:,indices] == x['probs']).all())
      self.assertTrue((confs[indices] == x['confidence']).all())
    self.assertEqual(i, 0)
    print 'iterating with indices took %f' % (time.time() - beg)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(markers_set('test_creation_destruction'))
  suite.addTest(markers_set('test_read_ssc'))
  suite.addTest(markers_set('test_gdo'))
  #--
  ## suite.addTest(markers_set('test_speed'))
  ## suite.addTest(markers_set('test_speed_gdo'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
