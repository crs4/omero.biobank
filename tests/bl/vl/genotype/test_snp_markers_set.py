import unittest
import time, os

import random

import numpy as np
import itertools as it

from bl.vl.kb import KnowledgeBase as KB

OME_HOST   = os.getenv('OME_HOST', 'localhost')
OME_USER   = os.getenv('OME_USER', 'root')
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
    conf = {'label' : 'TEST-%f' % time.time(),
            'description' : 'unit test garbage'}
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

  def create_alignments(self, mset, ref_genome):
    mset.load_markers()
    self.assertTrue(len(mset) > 0)
    aligns = [(m[0],
               random.randint(1,26), 1 + i*1000, True,
               'A' if (i%2)== 0 else 'B', 1)
              for i, m in enumerate(mset.markers)]

    self.kb.align_snp_markers_set(mset, ref_genome, aligns, self.action)
    return [(a[1], a[2]) for a in aligns]

  def test_creation_destruction(self):
    N = 32
    lvs = self.create_markers(N)
    mset = self.create_snp_markers_set(lvs)
    mset.load_markers()
    self.assertEqual(len(mset), N)
    for lv, m in it.izip(lvs, mset.markers):
      self.assertEqual(lv[1], m[0])
    # FIXME this should really happen automatically...
    self.kb.gadpt.delete_snp_markers_set_tables(mset.id)

  def test_align(self):
    N = 16
    lvs = self.create_markers(N)
    mset = self.create_snp_markers_set(lvs)
    ref_genome = 'g' + ('%f' % time.time())[-14:]
    aligns = self.create_alignments(mset, ref_genome)
    mset.load_alignments(ref_genome)
    for a, m in it.izip(aligns, mset.get_markers_iterator()):
      self.assertEqual(a, m.position)

  def test_gdo(self):
    return
    # FIXME
    study = kb.get_study('TEST01')
    action = kb.create_an_action(study)
    mset = self.kb.get_snp_markers_set(label='FakeTaqSet01')
    mset.load_markers()

    conf = {'label' : 'taq-%03d' % i,
            'status' : kb.DataSampleStatus.USABLE,
            'action' : action,
            'snpMarkersSet' : mset}
    data_sample = kb.factory.create(kb.GenotypeDataSample, conf).save()
    probs, conf = make_fake_data(mset)
    do = kb.add_gdo_data_object(action, data_sample, probs, conf)

    probs1, conf1 = data_sample.resolve_to_data()
    #self.assertAlmostEqual((probs1-probs)xxx)
    #self.assertAlmostEqual((conf1-conf)xxx)
    s = kb.get_gdo_iterator(mset, data_samples=[data_sample])
    for i, x in enumerate(s):
      print x
    self.assertEqual(i, 1)
    s = kb.get_gdo_iterator(mset, data_samples=[data_sample],
                            indices=slice(0, len(mset)))
    for i, x in enumerate(s):
      print x
    self.assertEqual(i, 1)
    s = kb.get_gdo_iterator(mset, data_samples=[data_sample],
                            indices=slice(0, len(mset)))
    for i, x in enumerate(s):
      print x
    self.assertEqual(i, 1)




def suite():
  suite = unittest.TestSuite()
  suite.addTest(markers_set('test_creation_destruction'))
  suite.addTest(markers_set('test_align'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

