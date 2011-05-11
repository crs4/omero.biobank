import os, unittest, time
import itertools as it
from bl.vl.sample.kb import KBError
from bl.vl.genotype.kb.drivers.omero.proxy import Proxy

import bl.vl.utils as vlu

import numpy as np

import logging
logging.basicConfig(level=logging.DEBUG)


OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")

BATCH_SIZE=1000

class SafeProxy(Proxy):
  SNP_MARKER_DEFINITIONS_TABLE = 'UNIT_TESTING.' + Proxy.SNP_MARKER_DEFINITIONS_TABLE
  SNP_ALIGNMENT_TABLE         = 'UNIT_TESTING.' + Proxy.SNP_ALIGNMENT_TABLE
  SNP_SET_DEF_TABLE           = 'UNIT_TESTING.' + Proxy.SNP_SET_DEF_TABLE
  SNP_SET_TABLE               = 'UNIT_TESTING.' + Proxy.SNP_SET_TABLE

class TestMarkers(unittest.TestCase):
  def setUp(self):
    self.proxy = SafeProxy(OME_HOST, OME_USER, OME_PASS)

  def tearDown(self):
    for tn in [SafeProxy.SNP_MARKER_DEFINITIONS_TABLE,
               SafeProxy.SNP_ALIGNMENT_TABLE,
               SafeProxy.SNP_SET_DEF_TABLE,
               SafeProxy.SNP_SET_TABLE
               ]:

      self.proxy.delete_table(tn)

  def manipulate_snp_definition_table(self):
    self.proxy.delete_table(SafeProxy.SNP_MARKER_DEFINITIONS_TABLE)
    self.proxy.create_snp_marker_definitions_table()
    #--
    source  = 'src-%d' % int(time.time())
    context = 'cxt-%d' % int(time.time())
    op_vid  = vlu.make_vid()
    mds = [{'source' : source,
            'context': context,
            'release': '1.0',
            'label':   'foo-%06d' % i,
            'rs_label': 'rs-%06d' % i,
            'mask'    : 'GGATACATTTTATTGC[A/G]CTTGCAGAGTATTTTT'} for i in range(10)]
    vids = self.proxy.add_snp_marker_definitions(it.islice(mds, len(mds)), op_vid=op_vid)
    self.assertEqual(len(vids), len(mds))
    #--
    for sel in [None, '(source=="%s")' % source, '(context=="%s")' % context,
                '(op_vid=="%s")' % op_vid]:
      mrks = self.proxy.get_snp_marker_definitions(selector=sel)
      self.assertEqual(len(mrks), len(mds))
      dmds = {}
      for m in mds:
        dmds[m['label']] = m
      for x in mrks:
        m = dmds[x[4]] # label
        self.assertEqual(m['source'], x[1])
        self.assertEqual(m['context'],x[2])
        self.assertEqual(m['release'],x[3])
        self.assertEqual(m['rs_label'],  x[5])

  def manipulate_snp_alignment_table(self):
    self.proxy.delete_table(SafeProxy.SNP_ALIGNMENT_TABLE)
    self.proxy.create_snp_alignment_table()
    #-
    ref_genome = 'hg18'
    op_vid  = vlu.make_vid()
    mds = [{'marker_vid' : vlu.make_vid(),
            'ref_genome' : ref_genome,
            'chromosome' : np.random.random_integers(1, 24),
            'pos'        : np.random.random_integers(1, 100000),
            'global_pos' : 0,
            'strand'     : np.random.random_integers(0,1),
            'allele'     : "AB"[np.random.random_integers(0,1)],
            'copies'     : np.random.random_integers(0,3),
            } for i in range(10)]
    for x in mds:
      x['global_pos'] = 10**10 * x['chromosome'] + x['pos']
    self.proxy.add_snp_alignments(it.islice(mds, len(mds)), op_vid=op_vid)
    for sel in [None]:
      mrks = self.proxy.get_snp_alignments(selector=sel)
      self.assertEqual(len(mrks), len(mds))
      dmds = {}
      for m in mds:
        dmds[m['marker_vid']] = m
      for x in mrks:
        m = dmds[x[0]] # vid
        for i, k in enumerate(x.dtype.names):
          self.assertEqual(m[k], x[i])

  def manipulate_snp_set_table(self):
    #-
    self.proxy.delete_table(SafeProxy.SNP_SET_DEF_TABLE)
    self.proxy.delete_table(SafeProxy.SNP_SET_TABLE)
    self.proxy.create_snp_markers_set_table()
    self.proxy.create_snp_set_table()
    set_vid = vlu.make_vid()
    maker   = 'foomatic'
    model   = 'barfoo'
    op_vid  = vlu.make_vid()
    mds = [{'marker_vid' : vlu.make_vid(),
            'marker_indx' : i,
            'allele_flip' : [True, False][np.random.random_integers(0,1)],
            } for i in range(10)]
    set_vid = self.proxy.add_snp_markers_set(maker, model,op_vid=op_vid)
    self.proxy.fill_snp_markers_set(set_vid, it.islice(mds, len(mds)), op_vid=op_vid)
    for sel in [None,
                '(maker=="%s")&(model=="%s")' % (maker, model),
                ]:
      snp_sets = self.proxy.get_snp_markers_sets(selector=sel)
      self.assertEqual(len(snp_sets), 1)
      self.assertEqual(snp_sets[0][0], set_vid)
      self.assertEqual(snp_sets[0][1], maker)
      self.assertEqual(snp_sets[0][2], model)
      self.assertEqual(snp_sets[0][3], op_vid)
    for sel in [None,
                '(vid=="%s")' % set_vid,
                '(op_vid=="%s")' % op_vid
                ]:
      mrks = self.proxy.get_snp_markers_set(selector=sel)
      self.assertEqual(len(mrks), len(mds))
      dmds = {}
      for m in mds:
        dmds[m['marker_vid']] = m
      for x in mrks:
        m = dmds[x[1]] # marker_vid
        for i, k in enumerate(x.dtype.names):
          self.assertEqual(m[k], x[i])

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestMarkers('manipulate_snp_definition_table'))
  suite.addTest(TestMarkers('manipulate_snp_alignment_table'))
  suite.addTest(TestMarkers('manipulate_snp_set_table'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
