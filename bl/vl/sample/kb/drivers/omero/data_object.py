from wrapper import OmeroWrapper
import bl.vl.utils           as vlu
import bl.vl.utils.ome_utils as vluo

import bl.vl.sample.kb as kb

import omero.rtypes as ort
import time

class DataObject(OmeroWrapper, kb.DataObject):

  OME_TABLE = "OriginalFile"

  def __init__(self, from_=None, name=None, mime_type=None, path=None, sha1=None, size=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      if name is None or mime_type is None or path is None or sha1 is None or size is None:
        raise ValueError("DataObject name mime_type path sha1 and size cannot be None")
      ome_obj = ome_type()
      ome_obj.name = ort.wrap(name)
      ome_obj.path = ort.wrap(path)
      ome_obj.mimetype = ort.wrap(mime_type)
      ome_obj.sha1 = ort.wrap(sha1)
      ome_obj.size = ort.rlong(size)
    super(DataObject, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.name is None:
      raise kb.KBError("DataObject name can't be None")
    elif self.mimetype is None:
      raise kb.KBError("DataObject mimetype can't be None")
    elif self.path is None:
      raise kb.KBError("DataObject path can't be None")
    elif self.sha1 is None:
      raise kb.KBError("DataObject sha1 can't be None")
    else:
      super(DataObject, self).__handle_validation_errors__()




