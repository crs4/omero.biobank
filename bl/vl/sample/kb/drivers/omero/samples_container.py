import omero.rtypes as ort
import bl.vl.utils           as vlu
import bl.vl.utils.ome_utils as vluo

import bl.vl.sample.kb as kb

import time

from wrapper import OmeroWrapper
from action  import Action
from study   import Study
from result  import Result

import logging

logger = logging.getLogger()

#-----------------------------------------------------------
class SamplesContainer(Result, kb.SamplesContainer):

  OME_TABLE = "SamplesContainer"

  def __setup__(self, ome_obj, slots, barcode, virtual_container, **kw):
    if slots is None or barcode is None:
      raise ValueError('SamplesContainer slots and barcode cannot be None')
    # FIXME
    assert slots > 0
    ome_obj.slots = ort.rint(slots)
    ome_obj.barcode = ort.rstring(barcode)
    ome_obj.virtualContainer = ort.rbool(virtual_container)
    if kw.has_key('label'):
      ome_obj.label = ort.rstring(kw['label'])
    super(SamplesContainer, self).__setup__(ome_obj, **kw)

  def __init__(self, from_=None, slots=None,
               barcode=None, virtual_container=False, **kw):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      ome_obj = ome_type()
      self.__setup__(ome_obj, slots, barcode, virtual_container, **kw)
    super(SamplesContainer, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.slots is None:
      raise kb.KBError("SamplesContainer slots can't be None")
    elif self.barcode is None:
      raise kb.KBError("SamplesContainer barcode can't be None")
    elif self.virtualContainer is None:
      raise kb.KBError("SamplesContainer virtualContainer can't be None")
    else:
      raise super(SamplesContainer, self).__handle_validation_errors__()

#-----------------------------------------------------------
class TiterPlate(SamplesContainer, kb.TiterPlate):

  OME_TABLE = "TiterPlate"

  def __setup__(self, ome_obj, rows, columns, barcode, virtual_container, **kw):
    if rows is None or columns is None or barcode is None:
      raise ValueError('TiterPlate rows, columns barcode cannot be None')
    # FIXME
    assert rows > 0 and columns > 0
    ome_obj.rows    = ort.rint(rows)
    ome_obj.columns = ort.rint(columns)
    super(TiterPlate, self).__setup__(ome_obj, slots=(rows*columns),
                                      barcode=barcode,
                                      virtual_container=virtual_container,
                                      **kw)

  def __init__(self, from_=None, rows=None, columns=None,
               barcode=None,
               virtual_container=False,
               **kw):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      ome_obj = ome_type()
      self.__setup__(ome_obj, rows, columns, barcode, virtual_container, **kw)
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

  def __setup__(self, ome_obj, study, **kw):
    if study is None:
      raise ValueError('DataCollection study cannot be None')
    ome_obj.vid = ort.rstring(vlu.make_vid())
    ome_obj.creationDate = vluo.time2rtime(time.time())
    ome_obj.study = study.ome_obj

  def __init__(self, from_=None, study=None, **kw):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      ome_obj = ome_type()
      self.__setup__(ome_obj, study, **kw)
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

