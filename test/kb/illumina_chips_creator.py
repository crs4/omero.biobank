# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, time
import logging
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger()

class KBICObjectCreator(KBObjectCreator):
  def create_illumina_array_of_arrays(self, action=None):
    conf = self.create_collection_conf_helper(action)
    conf['rows'] =  8
    conf['columns'] =  12
    conf['barcode'] =  '9898989-%s' % time.time()
    conf['status']  = self.kb.ContainerStatus.READY
    c = self.kb.factory.create(self.kb.IlluminaArrayOfArrays, conf)
    return conf, c

  def create_illumina_bead_chip_array_conf_helper(self, action):
    if not action:
      aconf, action = self.create_action()
      self.kill_list.append(action.save())
    conf = {
      'content'       : self.kb.VesselContent.DNA,
      'status'        : self.kb.VesselStatus.CONTENTUSABLE,
      'assayType'     : self.kb.IlluminaAssayType.HumanOmniExpress_12v1_C
      'action'        : action
      }
    return conf

  def create_illumina_bead_chip_array(self, label, array_of_arrays,
                                      action=None):
    conf = self.create_illumina_bead_chip_array_conf_helper(action)
    conf['container'] = array_of_arrays
    conf['label'] = label
    a = self.kb.factory.create(self.kb.IlluminaBeadChipArray, conf)
    return conf, a











