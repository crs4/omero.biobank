from wrapper import OmeroWrapper

import omero.rtypes as ort

import bl.vl.utils           as vlu
import bl.vl.utils.ome_utils as vluo

import bl.vl.sample.kb as kb


import time

from study import Study

#----------------------------------------------------------------------
class ActionSetup(OmeroWrapper, kb.ActionSetup):

  OME_TABLE = "ActionSetup"

  def __init__(self, from_=None, label=None, **kw):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      if label is None:
        raise ValueError('ActionSetup label cannot be None')
      ome_obj = ome_type()
      ome_obj.vid = ort.rstring(vlu.make_vid())
      ome_obj.label = ort.rstring(label)
    super(ActionSetup, self).__init__(ome_obj, **kw)

#----------------------------------------------------------------------
class Action(OmeroWrapper, kb.Action):

  OME_TABLE = "Action"

  def __init__(self, from_=None, **kw):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_action = from_
    else:
      ome_action = ome_type()
      ome_action.vid = ort.rstring(vlu.make_vid())
      ome_action.beginTime = vluo.time2rtime(time.time())
    super(Action, self).__init__(ome_action, **kw)

  def __handle_validation_errors__(self):
    if self.id is None:
      raise kb.KBError("action id can't be None")
    elif self.beginTime is None:
      raise kb.KBError("action beginTime can't be None")
    elif self.actionCategory is None:
      raise kb.KBError("action actionCategory can't be None")
    elif self.operator is None:
      raise kb.KBError("action operator can't be None")
    elif self.context is None:
      raise kb.KBError("action context can't be None")
    else:
      raise kb.KBError("unkwon error")

  def __setattr__(self, name, value):
    if name == 'beginTime':
      return setattr(self.ome_obj, name, vluo.time2rtime(value))
    elif name == 'endTime':
      return setattr(self.ome_obj, name, vluo.time2rtime(value))
    elif name == 'setup':
      return setattr(self.ome_obj, name, value.ome_obj)
    elif name == 'device':
      return setattr(self.ome_obj, name, value.ome_obj)
    elif name == 'actionCategory':
      return setattr(self.ome_obj, name, value)
    else:
      return super(Action, self).__setattr__(name, value)

  def __getattr__(self, name):
    if name == 'beginTime':
      return vluo.rtime2time(self.ome_obj.beginTime)
    elif name == 'endTime':
      return vluo.rtime2time(self.ome_obj.endTime)
    elif name == 'setup':
      return ActionSetup(self.ome_obj.setup)
    elif name == 'device':
      return Device(self.ome_obj.device)
    elif name == 'context':
      return Study(self.ome_obj.context)
    elif name == 'actionCategory':
      return self.ome_obj.actionCategory
    else:
      return super(Action, self).__getattr__(name)

