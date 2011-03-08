import os, unittest, time
import itertools as it
from bl.lib.genotype.kb import KBError
import bl.lib.genotype.kb.drivers.omero         as okb
import bl.lib.genotype.kb.drivers.omero.markers as okbm
import bl.lib.genotype.kb.drivers.omero.utils   as okbu
import vl.lib.utils as vlu

import numpy as np

import logging
logging.basicConfig(level=logging.DEBUG)


OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")

BATCH_SIZE=1000

okbm.SNP_DEFINITION_TABLE = 'UNIT_TESTING.' + okbm.SNP_DEFINITION_TABLE
okbm.SNP_ALIGNMENT_TABLE  = 'UNIT_TESTING.' + okbm.SNP_ALIGNMENT_TABLE
okbm.SNP_SET_TABLE        = 'UNIT_TESTING.' + okbm.SNP_SET_TABLE

class TestMarkers(unittest.TestCase):
  def setUp(self):
    okbu.delete_table(OME_HOST, OME_USER, OME_PASS,
                      okbm.SNP_DEFINITION_TABLE)
    okbu.create_snp_definition_table(OME_HOST, OME_USER, OME_PASS,
                                     okbm.SNP_DEFINITION_TABLE)
    #-
    okbu.delete_table(OME_HOST, OME_USER, OME_PASS, okbm.SNP_ALIGNMENT_TABLE)
    okbu.create_snp_alignment_table(OME_HOST, OME_USER, OME_PASS,
                                    okbm.SNP_ALIGNMENT_TABLE)
    #-
    okbu.delete_table(OME_HOST, OME_USER, OME_PASS, okbm.SNP_SET_TABLE)
    okbu.create_snp_set_table(OME_HOST, OME_USER, OME_PASS,
                              okbm.SNP_SET_TABLE)

  def tearDown(self):
    for tn in [okbm.SNP_DEFINITION_TABLE,
               okbm.SNP_ALIGNMENT_TABLE,
               okbm.SNP_SET_TABLE
               ]:
      okbu.delete_table(OME_HOST, OME_USER, OME_PASS, tn)

  def manipulate_snp_definition_table(self):
    okb.open(OME_HOST, OME_USER, OME_PASS)
    source  = 'src-%d' % int(time.time())
    context = 'cxt-%d' % int(time.time())
    op_vid  = vlu.make_vid()
    mds = [{'source' : source,
            'context': context,
            'label':   'foo-%06d' % i,
            'rs_label': 'rs-%06d' % i,
            'mask'    : 'GGATACATTTTATTGC[A/G]CTTGCAGAGTATTTTT'} for i in range(10)]
    okb.extend_snp_definition_table(it.islice(mds, len(mds)), op_vid=op_vid)
    for sel in [None, '(source=="%s")' % source, '(context=="%s")' % context, '(op_vid=="%s")' % op_vid]:
      mrks = okb.get_snp_definition_table_rows(selector=sel)
      self.assertEqual(len(mrks), len(mds))
      dmds = {}
      for m in mds:
        dmds[m['label']] = m
      for x in mrks:
        m = dmds[x[3]] # label
        self.assertEqual(m['source'], x[1])
        self.assertEqual(m['context'],x[2])
        self.assertEqual(m['rs_label'],  x[4])
    okb.close()

  def manipulate_snp_alignment_table(self):
    okb.open(OME_HOST, OME_USER, OME_PASS)
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
    okb.extend_snp_alignment_table(it.islice(mds, len(mds)), op_vid=op_vid)
    for sel in [None,
                #'(source=="%s")' % source, '(context=="%s")' % context, '(op_vid=="%s")' % op_vid
                ]:
      mrks = okb.get_snp_alignment_table_rows(selector=sel)
      self.assertEqual(len(mrks), len(mds))
      dmds = {}
      for m in mds:
        dmds[m['marker_vid']] = m
      for x in mrks:
        m = dmds[x[0]] # vid
        for i, k in enumerate(x.dtype.names):
          self.assertEqual(m[k], x[i])
    okb.close()

  def manipulate_snp_set_table(self):
    okb.open(OME_HOST, OME_USER, OME_PASS)
    set_vid = vlu.make_vid()
    maker   = 'foomatic'
    model   = 'barfoo'
    op_vid  = vlu.make_vid()
    mds = [{'vid' : set_vid,
            'maker' : maker,
            'model' : model,
            'marker_vid' : vlu.make_vid(),
            'marker_indx' : i,
            'allele_flip' : [True, False][np.random.random_integers(0,1)],
            } for i in range(10)]
    okb.extend_snp_set_table(it.islice(mds, len(mds)), op_vid=op_vid)
    for sel in [None,
                '(maker=="%s")&(model=="%s")' % (maker, model),
                '(vid=="%s")' % set_vid,
                '(op_vid=="%s")' % op_vid
                ]:
      mrks = okb.get_snp_set_table_rows(selector=sel)
      self.assertEqual(len(mrks), len(mds))
      dmds = {}
      for m in mds:
        dmds[m['marker_vid']] = m
      for x in mrks:
        m = dmds[x[3]] # marker_vid
        for i, k in enumerate(x.dtype.names):
          self.assertEqual(m[k], x[i])
    okb.close()

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestMarkers('manipulate_snp_definition_table'))
  suite.addTest(TestMarkers('manipulate_snp_alignment_table'))
  suite.addTest(TestMarkers('manipulate_snp_set_table'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
