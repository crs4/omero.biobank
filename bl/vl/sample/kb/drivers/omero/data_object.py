from wrapper import OmeroWrapper
import bl.vl.utils           as vlu
import bl.vl.utils.ome_utils as vluo

import bl.vl.sample.kb as kb

import omero.rtypes as ort
import time

from sample import DataSample

class DataObject(OmeroWrapper, kb.DataObject):

  OME_TABLE = "DataObject"

  def __init__(self, from_=None, sample=None, mime_type=None,
               path=None, sha1=None, size=None, **kw):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      if sample is None or mime_type is None \
             or path is None or sha1 is None or size is None:
        raise ValueError("DataObject sample mime_type path sha1 and size cannot be None")
      ome_obj = ome_type()
      # This is to pacify OriginalFile request for a .name
      ome_obj.name = ort.wrap(sample.label)
      ome_obj.sample = sample.ome_obj
      ome_obj.path = ort.wrap(path)
      ome_obj.mimetype = ort.wrap(mime_type)
      ome_obj.sha1 = ort.wrap(sha1)
      ome_obj.size = ort.rlong(size)
    super(DataObject, self).__init__(ome_obj, **kw)

  def __handle_validation_errors__(self):
    if self.sample is None:
      raise kb.KBError("DataObject sample can't be None")
    elif self.mimetype is None:
      raise kb.KBError("DataObject mimetype can't be None")
    elif self.path is None:
      raise kb.KBError("DataObject path can't be None")
    elif self.sha1 is None:
      raise kb.KBError("DataObject sha1 can't be None")
    elif self.size is None:
      raise kb.KBError("DataObject size can't be None")
    else:
      super(DataObject, self).__handle_validation_errors__()

  def __setattr__(self, name, value):
    if name == 'sample':
      return setattr(self.ome_obj, name, value.ome_obj)
    else:
      return super(DataObject, self).__setattr__(name, value)

  def __getattr__(self, name):
    #FIXME passing proxy should be handled automatically, ideally by
    # some generic implementation of __getattr__ configured with some
    # object specific behaviour rules, e.g., self.get_behaviour[name]
    if name == 'sample':
      return DataSample(getattr(self.ome_obj, name), proxy=self.proxy)
    else:
      return super(DataObject, self).__getattr__(name)

