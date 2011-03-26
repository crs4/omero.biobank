from wrapper import OmeroWrapper
import vl.lib.utils           as vlu
import vl.lib.utils.ome_utils as vluo

import bl.lib.sample.kb as kb

import omero.rtypes as ort
import time


class Device(OmeroWrapper, kb.Device):

  OME_TABLE = "Device"

  def __init__(self, from_=None):
    ome_type = Device.get_ome_type()
    if isinstance(from_, ome_type):
      ome_device = from_
    else:
      ome_device = ome_type()
      ome_device.vid = ort.rstring(vlu.make_vid())
    super(Device, self).__init__(ome_device)


