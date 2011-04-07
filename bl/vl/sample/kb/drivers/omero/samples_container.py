import omero.rtypes as ort
import vl.lib.utils           as vlu
import vl.lib.utils.ome_utils as vluo

import bl.lib.sample.kb as kb

import time

from wrapper import OmeroWrapper
from action  import Action
from study   import Study

#-----------------------------------------------------------
class SamplesContainer(OmeroWrapper, kb.SamplesContainer):

  OME_TABLE = "SamplesContainer"

  def __init__(self, from_=None, slots=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      if slots is None:
        raise ValueError('SamplesContainer slots cannot be None')
      # FIXME
      assert slots > 0
      ome_obj = ome_type()
      ome_obj.vid = ort.rstring(vlu.make_vid())
      ome_obj.slots = ort.rint(slots)
    super(SamplesContainer, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.slots is None:
      raise kb.KBError("SamplesContainer slots can't be None")
    elif self.barcode is None:
      raise kb.KBError("SamplesContainer barcode can't be None")
    elif self.virtualContainer is None:
      raise kb.KBError("SamplesContainer virtualContainer can't be None")
    else:
      raise kb.KBError("unkwon error")

#-----------------------------------------------------------
class TiterPlate(SamplesContainer, kb.TiterPlate):

  OME_TABLE = "TiterPlate"

  def __init__(self, from_=None, rows=None, columns=None,
               barcode=None,
               virtual_container=False):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      if rows is None or columns is None or barcode is None:
        raise ValueError('TiterPlate rows, columns barcode cannot be None')
      # FIXME
      assert rows > 0 and columns > 0
      ome_obj = ome_type()
      ome_obj.vid = ort.rstring(vlu.make_vid())
      ome_obj.rows    = ort.rint(rows)
      ome_obj.columns = ort.rint(columns)
      ome_obj.slots   = ort.rint(rows * columns)
      ome_obj.barcode = ort.rstring(barcode)
      ome_obj.virtualContainer = ort.rbool(virtual_container)
    super(TiterPlate, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.rows is None:
      raise kb.KBError("TiterPlate rows can't be None")
    elif self.columns is None:
      raise kb.KBError("TiterPlate columns can't be None")
    else:
      super(TiterPlate, self).__handle_validation_errors__()


#-----------------------------------------------------------
class DataCollection(OmeroWrapper, kb.DataCollection):

  OME_TABLE = "DataCollection"

  def __setup__(self, ome_obj):
    ome_obj.vid = ort.rstring(vlu.make_vid())
    ome_obj.creationDate = vluo.time2rtime(time.time())

  def __init__(self, from_=None, study=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      if study is None:
        raise ValueError('DataCollection study cannot be None')
      # FIXME
      ome_obj = ome_type()
      self.__setup__(ome_obj)
      ome_obj.study = study.ome_obj
    super(DataCollection, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.study is None:
      raise kb.KBError("DataCollection study can't be None")
    else:
      super(DataCollection, self).__handle_validation_errors__()

  def __getattr__(self, name):
    if name == 'study':
      return Study(getattr(self.ome_obj, name))
    else:
      return super(DataCollection, self).__getattr__(name)

