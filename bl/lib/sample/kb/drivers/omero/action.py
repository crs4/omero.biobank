from wrapper import OmeroWrapper

import omero.rtypes as ort

import vl.lib.utils           as vlu
import vl.lib.utils.ome_utils as vluo

import bl.lib.sample.kb as kb


import time

from study import Study

#----------------------------------------------------------------------
class ActionSetup(OmeroWrapper, kb.ActionSetup):

  OME_TABLE = "ActionSetup"

  def __init__(self, from_=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      ome_obj = ome_type()
      ome_obj.vid = ort.rstring(vlu.make_vid())
    super(ActionSetup, self).__init__(ome_obj)
#----------------------------------------------------------------------
class Device(OmeroWrapper, kb.Device):

  OME_TABLE = "Device"

  def __init__(self, from_=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      ome_obj = ome_type()
      ome_obj.vid = ort.rstring(vlu.make_vid())
    super(Device, self).__init__(ome_obj)

#----------------------------------------------------------------------
class Action(OmeroWrapper, kb.Action):

  OME_TABLE = "Action"

  def __init__(self, from_=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_action = from_
    else:
      ome_action = ome_type()
      ome_action.vid = ort.rstring(vlu.make_vid())
      ome_action.beginTime = vluo.time2rtime(time.time())
    super(Action, self).__init__(ome_action)

  def __handle_validation_errors__(self):
    if self.beginTime is None:
      raise kb.KBError("action beginTime can't be None")
    elif self.actionType is None:
      raise kb.KBError("action actionType can't be None")
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
    elif name == 'actionType':
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
    elif name == 'actionType':
      return self.ome_obj.actionType
    else:
      return super(Action, self).__getattr__(name)

