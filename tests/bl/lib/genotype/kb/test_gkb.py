import os, unittest, time
import itertools as it
import vl.lib.utils as vlu

from bl.lib.genotype.kb import KnowledgeBase as gKB
import bl.lib.genotype.kb.drivers.omero.table_ops as otop
import bl.lib.genotype.kb.drivers.omero.markers as okbm

import logging
logging.basicConfig(level=logging.DEBUG)

OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")


BATCH_SIZE=1000

okbm.SNP_DEFINITION_TABLE = 'UNIT_TESTING.' + okbm.SNP_DEFINITION_TABLE
okbm.SNP_ALIGNMENT_TABLE  = 'UNIT_TESTING.' + okbm.SNP_ALIGNMENT_TABLE
okbm.SNP_SET_DEF_TABLE    = 'UNIT_TESTING.' + okbm.SNP_SET_DEF_TABLE
okbm.SNP_SET_TABLE        = 'UNIT_TESTING.' + okbm.SNP_SET_TABLE


class TestGKB(unittest.TestCase):
  def setUp(self):
    self.gkb = gKB(driver='omero')(OME_HOST, OME_USER, OME_PASS)

  def tearDown(self):
    for tn in [okbm.SNP_DEFINITION_TABLE,
               okbm.SNP_ALIGNMENT_TABLE,
               okbm.SNP_SET_DEF_TABLE,
               okbm.SNP_SET_TABLE
               ]:
      otop.delete_table(OME_HOST, OME_USER, OME_PASS, tn)

  def test_snp_definition_table(self):
    #-- low level operations
    otop.delete_table(OME_HOST, OME_USER, OME_PASS,
                      okbm.SNP_DEFINITION_TABLE)
    otop.create_snp_definition_table(OME_HOST, OME_USER, OME_PASS,
                                     okbm.SNP_DEFINITION_TABLE)
    #--
    source  = 'src-%d' % int(time.time())
    context = 'cxt-%d' % int(time.time())
    op_vid  = self.gkb.make_vid()
    mds = [{'source' : source,
            'context': context,
            'label':   'foo-%06d' % i,
            'rs_label': 'rs-%06d' % i,
            'mask'    : 'GGATACATTTTATTGC[A/G]CTTGCAGAGTATTTTT'}
           for i in range(10)]
    vids = self.gkb.add_snp_marker_definitions(it.islice(mds, len(mds)),
                                               op_vid=op_vid)
    self.assertEqual(len(vids), len(mds))
    #--
    for sel in [None, '(source=="%s")' % source, '(context=="%s")' % context,
                '(op_vid=="%s")' % op_vid]:
      mrks = self.gkb.get_snp_marker_definitions(selector=sel)
      self.assertEqual(len(mrks), len(mds))
      dmds = {}
      for m in mds:
        dmds[m['label']] = m
      for x in mrks:
        m = dmds[x[3]] # label
        self.assertEqual(m['source'],   x[1])
        self.assertEqual(m['context'],  x[2])
        self.assertEqual(m['rs_label'], x[4])

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestGKB('test_snp_definition_table'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

