from bl.vl.sample.kb     import KBError
from bl.vl.sample.kb     import KnowledgeBase as sKB
from bl.vl.individual.kb import KnowledgeBase as iKB

import json

#-----------------------------------------------------------------------------
#FIXME this should be factored out....

import logging, time

logger = logging.getLogger(__name__)
counter = 0
def debug_wrapper(f):
  def debug_wrapper_wrapper(*args, **kv):
    global counter
    now = time.time()
    counter += 1
    logger.debug('%s[%d] in' % (f.__name__, counter))
    res = f(*args, **kv)
    logger.debug('%s[%d] out (%f)' % (f.__name__, counter, time.time() - now))
    counter -= 1
    return res
  return debug_wrapper_wrapper
#-----------------------------------------------------------------------------

class BadRecord(Exception):
  def __init__(self, msg):
    self.msg = msg

  def __str__(self):
    return repr(self.msg)



class Core(object):
  """
  The common set of methods used by the importer's modules.
  """
  def __init__(self, host=None, user=None, passwd=None, keep_tokens=1):
    self.skb = sKB(driver='omero')(host, user, passwd, keep_tokens)
    self.ikb = iKB(driver='omero')(host, user, passwd, keep_tokens)
    self.acat_map    = self.skb.get_action_category_table()
    self.outcome_map = self.skb.get_result_outcome_table()
    self.sstatus_map = self.skb.get_sample_status_table()
    self.dtype_map   = self.skb.get_data_type_table()
    self.gender_map  = self.ikb.get_gender_table()
    self.logger = logger
    self.record_counter = 0

  @debug_wrapper
  def get_device(self, label, maker, model, release):
    device = self.skb.get_device(label)
    if not device:
      self.logger.debug('creating a device')
      device = self.skb.Device(label=label, maker=maker, model=model, release=release)
      device = self.skb.save(device)
    return device

  @debug_wrapper
  def get_action_setup(self, label, conf):
    """
    :param label:
    :type  label: str
    :param conf:
    :type conf:  a python dict amenable to be json-ized
    :rtype: a skb.ActionSetup proxy to a saved ActionSetup object in VL
    """
    asetup = self.skb.get_action_setup_by_label(label=label)
    if not asetup:
      asetup = self.skb.ActionSetup(label=label)
      asetup.conf = json.dumps(conf)
      asetup = self.skb.save(asetup)
    return asetup

  def get_study_by_label(self, label):
    study = self.skb.get_study_by_label(label)
    if not study:
      study = self.skb.save(self.skb.Study(label=label))
    return study

  @debug_wrapper
  def create_action_helper(self, aklass, description,
                           study, device, asetup, acat, operator, target=None):
    action = aklass()
    action.setup, action.device, action.actionCategory = asetup, device, acat
    action.operator, action.context, action.description = operator, study, description
    if target:
      action.target = target
    try:
      return self.skb.save(action)
    except KBError, e:
      msg = 'got an error: %s\n\taction: %s\n\tome_obj: %s' % (e, action, action.ome_obj)
      self.logger.error(msg)
      raise KBError(msg)

