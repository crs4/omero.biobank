import omero.rtypes as ort
import vl.lib.utils           as vlu
import vl.lib.utils.ome_utils as vluo

import bl.lib.sample.kb as kb

import time

from wrapper import OmeroWrapper
from action         import Action

class Result(OmeroWrapper, kb.Result):

  OME_TABLE = "Result"

  def __init__(self, from_=None):
    ome_type = self.get_ome_type()
    if isinstance(from_, ome_type):
      ome_result = from_
    else:
      ome_result = ome_type()
      ome_result.vid = ort.rstring(vlu.make_vid())
      ome_result.creationDate = vluo.time2rtime(time.time())
    super(Result, self).__init__(ome_result)

  def __handle_validation_errors__(self):
    if self.creationDate is None:
      raise kb.KBError("Result creationDate can't be None")
    elif self.action is None:
      raise kb.KBError("Result action can't be None")
    else:
      raise kb.KBError("unkwon error")

  def __setattr__(self, name, value):
    if name == 'action':
      return setattr(self.ome_obj, name, value.ome_obj)
    elif name == 'creationDate':
      return setattr(self.ome_obj, name, vluo.time2rtime(value))
    elif name == 'outcome':
      return setattr(self.ome_obj, name, value)
    else:
      return super(Result, self).__setattr__(name, value)

  def __getattr__(self, name):
    if name == 'action':
      return Action(self.ome_obj.action)
    elif name == 'outcome':
      return self.ome_obj.outcome
    elif name == 'creationDate':
      return vluo.rtime2time(self.ome_obj.creationDate)
    else:
      return super(Result, self).__getattr__(name)


