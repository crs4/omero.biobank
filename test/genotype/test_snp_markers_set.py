# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, time, os, random
import itertools as it
import tempfile
import numpy as np

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.kb.drivers.omero.genotyping import Marker

from bl.core.io import MessageStreamWriter
import bl.core.gt.messages.SnpCall as SnpCall

import bl.vl.genotype.io as gio


OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'root')
OME_PASSWD = os.getenv('OME_PASSWD', 'romeo')

PAYLOAD_MSG_TYPE = 'core.gt.messages.SampleSnpCall'


def make_fake_data(mset, add_nan=False):
  n = len(mset)
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


def make_fake_ssc(mset, sample_id, probs, conf, fn):
  header = {'markers_set' : mset.label, 'sample_id':  sample_id}
  stream = MessageStreamWriter(fn, PAYLOAD_MSG_TYPE, header)
  labels = mset.markers['label']
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
    self.kill_list.reverse()
    for x in self.kill_list:
      self.kb.delete(x)
    self.kill_list = []

  def __create_snp_markers_set(self, N):
    label = 'ams-%f' % time.time()
    maker, model, release = 'FOO', 'FOO1', '%f' % time.time()
    rows = [('M%d' % i, 'AC[A/G]GT', i, False) for i in xrange(N)]
    mset = self.kb.create_snp_markers_set(
      label, maker, model, release, N, iter(rows), self.action
      )
    self.kill_list.append(mset)
    return mset, rows

  def __create_alignments(self, mset, ref_genome, n_duplicates):
    if not mset.has_markers():
      mset.load_markers()
    self.assertTrue(len(mset) > 0)
    n_aligns = len(mset.markers) + n_duplicates
    pos = []
    def insert_duplicates(markers):
      count = 0
      for i, m in enumerate(markers):
        n_copies = 1
        if count < n_duplicates:
          count += 1
          n_copies = 2
          r = (m[0], random.randint(1, 26), 22 + i*1000, True,
               'A' if (i % 2) == 0 else 'B', n_copies)
          yield r
        r = (m[0], random.randint(1, 26), 1 + i*1000, True,
             'A' if (i % 2) == 0 else 'B', n_copies)
        pos.append((0, 0) if n_copies > 1 else (r[1], r[2]))
        yield r
    aligns = list(insert_duplicates(mset.markers))
    random.shuffle(aligns)
    self.kb.align_snp_markers_set(mset, ref_genome, aligns, self.action)
    return pos

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
    probs, confs = make_fake_data(data_sample.snpMarkersSet, add_nan)
    do = self.kb.add_gdo_data_object(self.action, data_sample, probs, confs)
    self.kill_list.append(do)
    return probs, confs

  def __create_aligned_mset(self, N, N_dups, ref_genome):
    mset, _ = self.__create_snp_markers_set(N)
    pos = self.__create_alignments(mset, ref_genome, N_dups)
    return mset, pos

  def test_creation_destruction(self):
    N = 32
    mset, rows = self.__create_snp_markers_set(N)
    mset.load_markers()
    self.assertEqual(len(mset), N)
    for r, m in it.izip(rows, mset.markers):
      self.assertEqual(len(r)+2, len(m))
      for i, x in enumerate(r):
        self.assertEqual(x, m[i+1])

  def test_get_markers_iterator(self):
    N = 32
    mset, _ = self.__create_snp_markers_set(N)
    mset.load_markers()
    for mdef, m in it.izip(mset.markers, mset.get_markers_iterator()):
      self.assertTrue(isinstance(m, Marker))
      self.assertEqual(mdef['vid'], m.id)

  def test_align(self):
    N = 16
    N_dups = 4
    ref_genome = 'g' + ('%f' % time.time())[-14:]
    mset, pos = self.__create_aligned_mset(N, N_dups, ref_genome)
    mset.load_alignments(ref_genome)
    for p, m in it.izip(pos, mset.get_markers_iterator()):
      self.assertEqual(p, m.position)

  def test_read_ssc(self):
    N = 16
    N_dups = 4
    ref_genome = 'g' + ('%f' % time.time())[-14:]
    mset, pos = self.__create_aligned_mset(N, N_dups, ref_genome)
    mset.load_markers()
    probs, confs = make_fake_data(mset)
    sample_id = 'ffoo-%f' % time.time()
    fn = tempfile.NamedTemporaryFile().name
    make_fake_ssc(mset, sample_id, probs, confs, fn)
    probs_1, confs_1 = gio.read_ssc(fn, mset)
    self.assertAlmostEqual(np.sum(np.abs(probs - probs_1)), 0.0)
    self.assertAlmostEqual(np.sum(np.abs(confs - confs_1)), 0.0)

  def test_gdo(self):
    N = 32
    mset, _ = self.__create_snp_markers_set(N)
    mset.load_markers()
    data_sample = self.__create_data_sample(mset, 'foo-data')
    probs, confs = self.__create_data_object(data_sample)
    probs1, confs1 = data_sample.resolve_to_data()
    self.assertTrue((probs == probs1).all())
    self.assertTrue((confs == confs1).all())
    s = self.kb.get_gdo_iterator(mset, data_samples=[data_sample])
    for i, x in enumerate(s):
      self.assertTrue((probs == x['probs']).all())
      self.assertTrue((confs == x['confidence']).all())
    self.assertEqual(i, 0)
    indices = slice(N/4, N/2)
    s = self.kb.get_gdo_iterator(
      mset, data_samples=[data_sample], indices=indices
      )
    for i, x in enumerate(s):
      self.assertTrue((probs[:,indices] == x['probs']).all())
      self.assertTrue((confs[indices] == x['confidence']).all())
    self.assertEqual(i, 0)

  def test_define_range_selector(self):
    N, N_dups = 16, 0
    ref_genome = 'g' + ('%f' % time.time())[-14:]
    mset, pos = self.__create_aligned_mset(N, N_dups, ref_genome)
    mset.load_alignments(ref_genome)
    pos.sort()
    if len(pos) > 2:
      low_pos, high_pos = pos[1], pos[-2]
    gc_range = (low_pos, high_pos)
    range_sel = self.kb.SNPMarkersSet.define_range_selector(mset, gc_range)
    i = 0
    for (i, m) in enumerate(mset.get_markers_iterator()):
      if i in range_sel:
        self.assertTrue(low_pos <= m.position <= high_pos)
      else:
        self.assertTrue(low_pos > m.position or high_pos < m.position)

  def test_intersect(self):
    ref_genome = 'g' + ('%f' % time.time())[-14:]
    N1 = 16
    M1 = 2
    N2 = N1/2
    M2 = 1
    mset1, _ = self.__create_snp_markers_set(N1)
    mset1.load_markers()
    aligns = [(m[0], random.randint(1,26), 1 + i*2000, True, 'A', 1)
              for i, m in enumerate(mset1.markers)]
    for i in range(M1):
      aligns[i] = (aligns[i][0], 0, 0, True, 'A', 0)
    self.kb.align_snp_markers_set(mset1, ref_genome, aligns, self.action)
    mset2, _ = self.__create_snp_markers_set(N2)
    mset2.load_markers()
    aligns = [(m[0], a[1], a[2], a[3], a[4], a[5])
              for m, a in it.izip(mset2.markers, aligns[:len(mset2)])]
    for i in range(M2):
      aligns[i] = (aligns[i][0], 0, 0, True, 'A', 0)
    self.kb.align_snp_markers_set(mset2, ref_genome, aligns, self.action)
    mset1.load_alignments(ref_genome)
    mset2.load_alignments(ref_genome)
    idx1, idx2 = self.kb.SNPMarkersSet.intersect(mset1, mset1)
    self.assertTrue(np.array_equal(idx1, idx2))
    self.assertEqual(len(idx1), len(mset1))
    self.assertEqual(len(idx1), N1)
    idx1, idx2 = self.kb.SNPMarkersSet.intersect(mset1, mset2)
    self.assertEqual(len(idx1), len(idx2))
    self.assertEqual(len(idx1), N2 - max(M1, M2))
    for i,j in it.izip(idx1, idx2):
      m1, m2 = mset1[i], mset2[j]
      self.assertEqual(m1.position, m2.position)
      self.assertTrue(m1.position > (0,0))

  def test_speed(self):
    ref_genome = 'g' + ('%f' % time.time())[-14:]
    N1 = 1024*1024
    N2 = N1/2
    beg = time.time()
    print ''
    print 'creating %d markers took %f' % (N1, time.time() - beg)
    beg = time.time()
    mset1, _ = self.__create_snp_markers_set(N1)
    print 'creating a markers set with %d markers took %f' % (
      N1, time.time() - beg
      )
    beg = time.time()
    mset1.load_markers()
    print 'loading markers took %f' % (time.time() - beg)
    beg = time.time()
    aligns = [(m[0], random.randint(1,26), 1 + i*2000, True, 'A', 1)
              for i, m in enumerate(mset1.markers)]
    print 'creating %d aligns took %f' % (N1, time.time() - beg)
    beg = time.time()
    self.kb.align_snp_markers_set(mset1, ref_genome, aligns, self.action)
    print 'saving  %d aligns took %f' % (N1, time.time() - beg)
    beg = time.time()
    mset1.load_alignments(ref_genome)
    print 'loading  %d aligns took %f' % (N1, time.time() - beg)
    beg = time.time()
    idx1, idx2 = self.kb.SNPMarkersSet.intersect(mset1, mset1)
    print 'intersecting  %d aligns took %f' % (N1, time.time() - beg)

  def test_speed_gdo(self):
    N = 934968
    beg = time.time()
    print ''
    print 'creating %d markers took %f' % (N, time.time() - beg)
    mset, _ = self.__create_snp_markers_set(N)
    beg = time.time()
    mset.load_markers()
    print 'loading %d markers took %f' % (N, time.time() - beg)
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
    s = self.kb.get_gdo_iterator(mset, data_samples=[data_sample])
    for i, x in enumerate(s):
      self.assertTrue((probs == x['probs']).all())
      self.assertTrue((confs == x['confidence']).all())
    self.assertEqual(i, 0)
    print 'iterating took %f' % (time.time() - beg)
    indices = slice(N/4, N/2)
    beg = time.time()
    s = self.kb.get_gdo_iterator(
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
  suite.addTest(markers_set('test_get_markers_iterator'))
  suite.addTest(markers_set('test_align'))
  suite.addTest(markers_set('test_read_ssc'))
  suite.addTest(markers_set('test_gdo'))
  suite.addTest(markers_set('test_define_range_selector'))
  suite.addTest(markers_set('test_intersect'))
  #--
  ## suite.addTest(markers_set('test_speed'))
  ## suite.addTest(markers_set('test_speed_gdo'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
