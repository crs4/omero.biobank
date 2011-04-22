from wrapper import OmeroWrapper
import bl.vl.utils           as vlu
import bl.vl.utils.ome_utils as vluo

import bl.vl.sample.kb as kb

import omero.rtypes as ort
import time


class Device(OmeroWrapper, kb.Device):

  OME_TABLE = "Device"

  def __setup__(self, ome_obj, label, maker, model, release):
    if label is None or maker is None or model is None or release is None:
      raise ValueError("Device label maker model and release cannot be None")
    ome_obj.vid   = ort.rstring(vlu.make_vid())
    ome_obj.label = ort.rstring(label)
    ome_obj.maker = ort.rstring(maker)
    ome_obj.model = ort.rstring(model)
    ome_obj.release = ort.rstring(release)
    ome_obj.deviceUK = vluo.make_unique_key(label, maker, model, release)

  def __init__(self, from_=None, label=None, maker=None, model=None, release=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      ome_obj = ome_type()
      self.__setup__(ome_obj, label, maker, model, release)
    super(Device, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.label is None:
      raise kb.KBError("Device label can't be None")
    elif self.maker is None:
      raise kb.KBError("Device maker can't be None")
    elif self.model is None:
      raise kb.KBError("Device model can't be None")
    elif self.release is None:
      raise kb.KBError("Device release can't be None")
    else:
      super(Device, self).__handle_validation_errors__()

  def __setattr__(self, name, value):
    if name == 'label':
      setattr(self.ome_obj, 'deviceUK',
              vluo.make_unique_key(value, self.maker, self.model, self.release))
      return super(Device, self).__setattr__(name, value)
    elif name == 'maker':
      setattr(self.ome_obj, 'deviceUK',
              vluo.make_unique_key(self.label, value, self.model, self.release))
      return super(Device, self).__setattr__(name, value)
    elif name == 'model':
      setattr(self.ome_obj, 'deviceUK',
              vluo.make_unique_key(self.label, self.maker, value, self.release))
      return super(Device, self).__setattr__(name, value)
    elif name == 'release':
      setattr(self.ome_obj, 'deviceUK',
              vluo.make_unique_key(self.label, self.maker, self.model, value))
      return super(Device, self).__setattr__(name, value)
    else:
      return super(Device, self).__setattr__(name, value)



