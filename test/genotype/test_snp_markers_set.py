# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, time, os, random
import itertools as it
import tempfile
import numpy as np

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.kb.drivers.omero.genomics import MSET_TABLE_COLS_DTYPE

from bl.core.io import MessageStreamWriter
import bl.core.gt.messages.SnpCall as SnpCall

import bl.vl.genotype.io as gio
import bl.vl.utils as vlu


OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'root')
OME_PASSWD = os.getenv('OME_PASSWD', 'romeo')

PAYLOAD_MSG_TYPE = 'core.gt.messages.SampleSnpCall'


def make_fake_data(n, add_nan=False):
  probs = 0.5 * np.cast[np.float32](np.random.random((2, n)))
  confs = np.cast[np.float32](np.random.random(n))
  if add_nan:
    rand_indices = np.random.random_integers(
      0, len(probs[0]) - 1, len(probs[0]) / 2
      )
    for x in set(rand_indices):
      probs[0][x] = np.nan
      probs[1][x] = np.nan
  return probs, confs


def make_fake_ssc(mset, labels, sample_id, probs, conf, fn):
  header = {'markers_set' : mset.label, 'sample_id':  sample_id}
  stream = MessageStreamWriter(fn, PAYLOAD_MSG_TYPE, header)
  for l, p_AA, p_BB, c in  it.izip(labels, probs[0], probs[1], conf):
    p_AB = 1.0 - (p_AA + p_BB)
    w_aa, w_ab, w_bb = p_AA, p_AB, p_BB
    stream.write({
      'sample_id': sample_id,
      'snp_id': l,
      'call': SnpCall.NOCALL, # we will not test this anyway
      'confidence': float(c),
      'sig_A': float(p_AA),
      'sig_B': float(p_BB),
      'w_AA': float(w_aa),
      'w_AB': float(w_ab),
      'w_BB': float(w_bb),
      })
  stream.close()


class markers_set(unittest.TestCase):

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

  def __create_markers_set(self, N):
    label = 'ams-%f' % time.time()
    maker, model, release = 'FOO', 'FOO1', '%f' % time.time()
    vid = vlu.make_vid()
    rows = np.array([('M%d' % i, i, 'AC[A/G]GT', False, vid) 
                     for i in xrange(N)],
                    dtype=MSET_TABLE_COLS_DTYPE)
    mset = self.kb.genomics.create_markers_array(
      label, maker, model, release, rows, self.action
      )
    self.kill_list.append(mset)
    return mset, rows

  def __create_data_sample(self, mset, label):
    conf = {
      'label': label,
      'status': self.kb.DataSampleStatus.USABLE,
      'action': self.action,
      'snpMarkersSet': mset,
      }
    data_sample = self.kb.factory.create(self.kb.GenotypeDataSample,
                                         conf).save()
    self.kill_list.append(data_sample)
    return data_sample

  def __create_data_object(self, data_sample, add_nan=False):
    n = self.kb.genomics.get_number_of_markers(data_sample.snpMarkersSet)
    probs, confs = make_fake_data(n, add_nan)
    do = self.kb.genomics.add_gdo_data_object(self.action, 
                                              data_sample, probs, confs)
    self.kill_list.append(do)
    return probs, confs

  def test_creation_destruction(self):
    N = 32
    mset, rows = self.__create_markers_set(N)
    markers = self.kb.genomics.get_markers_array_rows(mset)
    self.assertEqual(len(markers), N)
    for r, m in it.izip(rows, markers):
      self.assertEqual(len(r), len(m))
      for i, x in enumerate(r):
        self.assertEqual(x, m[i])

  def test_read_ssc(self):
    N = 16
    N_dups = 4
    mset, rows = self.__create_markers_set(N)
    probs, confs = make_fake_data(N)
    sample_id = 'ffoo-%f' % time.time()
    fn = tempfile.NamedTemporaryFile().name
    make_fake_ssc(mset, rows['label'], sample_id, probs, confs, fn)
    probs_1, confs_1 = gio.read_ssc(fn, mset)
    self.assertAlmostEqual(np.sum(np.abs(probs - probs_1)), 0.0)
    self.assertAlmostEqual(np.sum(np.abs(confs - confs_1)), 0.0)

  def test_gdo(self):
    N = 32
    mset, _ = self.__create_markers_set(N)
    data_sample = self.__create_data_sample(mset, 'foo-data')
    probs, confs = self.__create_data_object(data_sample)
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
    mset1, _ = self.__create_markers_set(N1)
    print 'creating a markers set with %d markers took %f' % (
      N1, time.time() - beg
      )

  def test_speed_gdo(self):
    N = 934968
    beg = time.time()
    print ''
    print 'creating %d markers took %f' % (N, time.time() - beg)
    mset, _ = self.__create_markers_set(N)
    beg = time.time()
    data_sample = self.__create_data_sample(mset, 'foo-data')
    print 'creating a data sample took %f' % (time.time() - beg)
    beg = time.time()
    probs, confs = self.__create_data_object(data_sample)
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
