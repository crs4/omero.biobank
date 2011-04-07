from wrapper import OmeroWrapper
import bl.vl.utils           as vlu
import bl.vl.utils.ome_utils as vluo

import bl.vl.sample.kb as kb

import omero.rtypes as ort
import time


class Device(OmeroWrapper, kb.Device):

  OME_TABLE = "Device"

  def __setup__(self, ome_obj, vendor, model, release):
    ome_obj.vid    = ort.rstring(vlu.make_vid())
    ome_obj.vendor = ort.rstring(vendor)
    ome_obj.model  = ort.rstring(model)
    ome_obj.release = ort.rstring(release)
    ome_obj.deviceUK = vluo.make_unique_key(vendor, model, release)

  def __init__(self, from_=None, vendor=None, model=None, release=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      if vendor is None or model is None or release is None:
        raise ValueError("Device vendor model and release cannot be None")
      ome_obj = ome_type()
      self.__setup__(ome_obj, vendor, model, release)
    super(Device, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.vendor is None:
      raise kb.KBError("Device vendor can't be None")
    elif self.model is None:
      raise kb.KBError("Device model can't be None")
    elif self.release is None:
      raise kb.KBError("Device release can't be None")
    else:
      super(Device, self).__handle_validation_errors__()

  def __setattr__(self, name, value):
    if name == 'vendor':
      setattr(self.ome_obj, 'deviceUK',
              vluo.make_unique_key(value, self.model, self.release))
      return super(Device, self).__setattr__(name, value)
    elif name == 'model':
      setattr(self.ome_obj, 'deviceUK',
              vluo.make_unique_key(self.vendor, value, self.release))
      return super(Device, self).__setattr__(name, value)
    elif name == 'release':
      setattr(self.ome_obj, 'deviceUK',
              vluo.make_unique_key(self.vendor, self.model, value))
      return super(Device, self).__setattr__(name, value)
    else:
      return super(Device, self).__setattr__(name, value)



