# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, time
import logging
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger()

from kb_object_creator import KBObjectCreator

class KBACObjectCreator(KBObjectCreator):

  def create_affymetrix_array(self, action=None):
    conf = self.create_vessel_conf_helper(action)
    conf['label'] = 'aa-%s'  % time.time()
    conf['assayType'] = self.kb.AffymetrixAssayType.GENOMEWIDESNP_6
    aa = self.kb.factory.create(self.kb.AffymetrixArray, conf)
    return conf, aa

  def create_affymetrix_cel(self, action=None):
    conf = self.create_data_sample_conf_helper(action)
    conf['arrayType'] = self.kb.AffymetrixCelArrayType.GENOMEWIDESNP_6
    ds = self.kb.factory.create(self.kb.AffymetrixCel, conf)
    return conf, ds












