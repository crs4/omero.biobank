# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest, time
import logging
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger()

from kb_object_creator import KBObjectCreator

class KBICObjectCreator(KBObjectCreator):
  def create_illumina_array_of_arrays(self, action=None, rows=8, cols=2):
    conf = self.create_collection_conf_helper(action)
    conf['rows'] =  rows
    conf['columns'] =  cols
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
      'assayType'     : self.kb.IlluminaAssayType.HumanOmniExpress_12v1_C,
      'action'        : action
      }
    return conf

  def create_illumina_bead_chip_array(self, label, array_of_arrays, slot=None,
                                      action=None):
    conf = self.create_illumina_bead_chip_array_conf_helper(action)
    conf['container'] = array_of_arrays
    conf['label'] = label
    if slot:
      conf['slot'] = slot
    a = self.kb.factory.create(self.kb.IlluminaBeadChipArray, conf)
    return conf, a

  def create_illumina_bead_chip_measure(self, action=None):
    conf = self.create_data_sample_conf_helper(action)
    m = self.kb.factory.create(self.kb.IlluminaBeadChipMeasure, conf)
    return conf, m

  def create_illumina_bead_chip_measures(self, red_channel=None,
                                         green_channel=None,
                                         action=None):
    conf = self.create_collection_conf_helper(action)
    conf['status']  = self.kb.ContainerStatus.READY
    if green_channel is None:
      _, green_channel = self.create_illumina_bead_chip_measure(action)
      self.kill_list.append(green_channel.save())
    if red_channel is None:
      _, red_channel =  self.create_illumina_bead_chip_measure(action)
      self.kill_list.append(red_channel.save())
    conf['greenChannel'] = green_channel
    conf['redChannel'] = red_channel
    m = self.kb.factory.create(self.kb.IlluminaBeadChipMeasures, conf)
    return conf, m











