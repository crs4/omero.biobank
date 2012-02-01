# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, time, os, random
import itertools as it

import numpy as np

from bl.vl.kb import KnowledgeBase as KB


OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'root')
OME_PASSWD = os.getenv('OME_PASSWD', 'romeo')


def make_fake_data(mset):
  n = len(mset)
  probs = 0.5 * np.cast[np.float32](np.random.random((2, n)))
  confs = np.cast[np.float32](np.random.random(n))
  return probs, confs


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

  def create_markers(self, N):
    def marker_generator():
      for i in range(N):
        label = 'A%f-%d' % (time.time(), i)
        yield (label, label, 'ACCA[A/B]TACCA')
    source, context, release = 'unit_testing', 'markers_set', '%f' % time.time()
    ref_rs_genome, dbsnp_build = 'foo-rs-genome', 123000
    lvs = self.kb.create_markers(source, context, release,
                                 ref_rs_genome, dbsnp_build,
                                 marker_generator(), self.action)
    return lvs

  def create_snp_markers_set(self, lvs):
    label = 'a-fake-marker-set'
    maker, model, release = 'FOO', 'FOO1', '%f' % time.time()
    markers_selection = [(v[1], i, False) for i, v in enumerate(lvs)]
    mset = self.kb.create_snp_markers_set(label, maker, model, release,
                                          len(lvs),
                                          markers_selection, self.action)
    self.kill_list.append(mset)
    return mset

  def create_alignments(self, mset, ref_genome, n_duplicates):
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
          r = (m[0], random.randint(1,26), 22 + i*1000, True,
               'A' if (i%2)== 0 else 'B', n_copies)
          yield r
        r = (m[0], random.randint(1,26), 1 + i*1000, True,
             'A' if (i%2)== 0 else 'B', n_copies)
        pos.append((0,0) if n_copies > 1 else (r[1], r[2]))
        yield r
    aligns = [x for x in insert_duplicates(mset.markers)]
    self.kb.align_snp_markers_set(mset, ref_genome, aligns, self.action)
    return pos

  def create_data_sample(self, mset, label):
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

  def create_data_object(self, data_sample):
    probs, confs = make_fake_data(data_sample.snpMarkersSet)
    do = self.kb.add_gdo_data_object(self.action, data_sample, probs, confs)
    self.kill_list.append(do)
    return probs, confs

  def test_creation_destruction(self):
    N = 32
    lvs = self.create_markers(N)
    mset = self.create_snp_markers_set(lvs)
    mset.load_markers()
    self.assertEqual(len(mset), N)
    for lv, m in it.izip(lvs, mset.markers):
      self.assertEqual(lv[1], m[0])
    # FIXME this should happen automatically
    self.kb.gadpt.delete_snp_markers_set_tables(mset.id)

  def test_align(self):
    N = 16
    N_dups = 4
    lvs = self.create_markers(N)
    mset = self.create_snp_markers_set(lvs)
    ref_genome = 'g' + ('%f' % time.time())[-14:]
    pos = self.create_alignments(mset, ref_genome, N_dups)
    mset.load_alignments(ref_genome)
    for p, m in it.izip(pos, mset.get_markers_iterator()):
      self.assertEqual(p, m.position)
    # FIXME this should happen automatically
    self.kb.gadpt.delete_snp_markers_set_tables(mset.id)

  def test_gdo(self):
    N = 16
    lvs = self.create_markers(N)
    mset = self.create_snp_markers_set(lvs)
    mset.load_markers()
    data_sample = self.create_data_sample(mset, 'foo-data')
    probs, confs = self.create_data_object(data_sample)
    probs1, confs1 = data_sample.resolve_to_data()
    self.assertTrue((probs == probs1).all())
    self.assertTrue((confs == confs1).all())
    s = self.kb.get_gdo_iterator(mset, data_samples=[data_sample])
    for i, x in enumerate(s):
      self.assertTrue((probs == x['probs']).all())
      self.assertTrue((confs == x['confidence']).all())
    self.assertEqual(i, 0)
    indices = slice(N/4, N/2)
    s = self.kb.get_gdo_iterator(mset, data_samples=[data_sample],
                                 indices=indices)
    for i, x in enumerate(s):
      self.assertTrue((probs[:,indices] == x['probs']).all())
      self.assertTrue((confs[indices] == x['confidence']).all())
    self.assertEqual(i, 0)
    # FIXME this should happen automatically
    self.kb.gadpt.delete_snp_markers_set_tables(mset.id)


def suite():
  suite = unittest.TestSuite()
  suite.addTest(markers_set('test_creation_destruction'))
  suite.addTest(markers_set('test_align'))
  suite.addTest(markers_set('test_gdo'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
