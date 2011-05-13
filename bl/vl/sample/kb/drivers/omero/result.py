import omero.rtypes as ort
import bl.vl.utils           as vlu
import bl.vl.utils.ome_utils as vluo

import bl.vl.sample.kb as kb

import time, sys

from wrapper import OmeroWrapper
from action  import Action

class Result(OmeroWrapper, kb.Result):

  OME_TABLE = "Result"

  def __setup__(self, ome_obj, **kw):
    ome_obj.vid = ort.rstring(vlu.make_vid())
    ome_obj.creationDate = vluo.time2rtime(time.time())

  def __init__(self, from_=None, **kw):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      ome_obj = ome_type()
      self.__setup__(ome_obj, **kw)
    super(Result, self).__init__(ome_obj, **kw)


  def __handle_validation_errors__(self):
    if self.vid is None:
      raise kb.KBError("Result vid can't be None")
    elif self.creationDate is None:
      raise kb.KBError("Result creationDate can't be None")
    elif self.action is None:
      raise kb.KBError("Result action can't be None")
    else:
      return super(Result, self).__handle_validation_errors__()

  def __setattr__(self, name, value):
    if name == 'action':
      return setattr(self.ome_obj, name, value.ome_obj)
    elif name == 'outcome':
      return setattr(self.ome_obj, name, value)
    elif name == 'creationDate':
      return setattr(self.ome_obj, name, vluo.time2rtime(value))
    else:
      return super(Result, self).__setattr__(name, value)

  def __getattr__(self, name):
    if name == 'creationDate':
      return vluo.rtime2time(self.ome_obj.creationDate)
    elif name == 'action':
      #FIXME:
      #obj = self.__upcast(self.ome_obj.action)
      #assert isinstance(obj, Action)
      #---
      # >>> type(self.ome_obj.action)
      # <class 'omero.model.ActionOnSampleI'>
      # >>> type(a).__name__
      # 'ActionOnSampleI'
      # >>>
      obj = Action(self.ome_obj.action, proxy=self.proxy)
      return obj
    elif name == 'outcome':
      return self.ome_obj.outcome
    else:
      return super(Result, self).__getattr__(name)
