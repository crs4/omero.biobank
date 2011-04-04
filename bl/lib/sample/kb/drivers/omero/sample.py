import omero.rtypes as ort
from wrapper import OmeroWrapper

import vl.lib.utils           as vlu
import vl.lib.utils.ome_utils as vluo

import bl.lib.sample.kb as kb

import time

from result import Result
from samples_container import SamplesContainer, TiterPlate, DataCollection

#------------------------------------------------------------
class Sample(Result, kb.Sample):

  OME_TABLE = "Sample"


#------------------------------------------------------------
class ContainerSlot(Result, kb.Result):

  OME_TABLE = "ContainerSlot"

  def __setup__(self, ome_obj, sample, container, slot_position):
    slots = container.slots
    assert slot_position >= 0 and  slot_position < slots
    assert isinstance(sample, BioSample)
    ome_obj.sample = sample.ome_obj
    ome_obj.container = container.ome_obj
    ome_obj.slotPosition = ort.rint(slot_position)
    ome_obj.contSlotUK = vluo.make_unique_key(container.id, slot_position)
    super(ContainerSlot, self).__setup__(ome_obj)

  def __init__(self, from_=None, sample=None, container=None, slot_position=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      if sample is None or container is None or slot_position is None:
        raise ValueError('ContainerSlot sample, container, slot_position cannot be None')
      if not isinstance(container, SamplesContainer):
        raise ValueError('ContainerSlot container should be a SamplesContainer instance')
      if not isinstance(sample, BioSample):
        raise ValueError('ContainerSlot sample should be a Sample instance')
      ome_obj = ome_type()
      self.__setup__(ome_obj, sample, container, slot_position)
      # FIXME
      #ome_obj.contLabelUK = vluo.make_unique_key(container.id, label)
    super(ContainerSlot, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.sample is None:
      raise kb.KBError("ContainerSlot sample can't be None")
    elif self.container is None:
      raise kb.KBError("ContainerSlot container can't be None")
    elif self.slotPosition is None:
      raise kb.KBError("ContainerSlot slotPosition can't be None")
    elif self.contSlotUK is None:
      raise kb.KBError("ContainerSlot contSlotUK can't be None")
    else:
      super(ContainerSlot, self).__handle_validation_errors__()

  def __getattr__(self, name):
    if name == 'sample':
      return Sample(getattr(self.ome_obj, name))
    elif name == 'container':
      return SamplesContainer(getattr(self.ome_obj, name))
    else:
      return super(ContainerSlot, self).__getattr__(name)

#------------------------------------------------------------
class PlateWell(ContainerSlot, kb.PlateWell):

  OME_TABLE = "PlateWell"

  def __init__(self, from_=None, sample=None, container=None, row=None, column=None, volume=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      if sample is None or container is None or row is None or column is None or volume is None:
        raise ValueError('PlateWell sample, container, row, column, and volume cannot be None')
      if not isinstance(container, TiterPlate):
        raise ValueError('PlateWell container should be a TiterPlate instance')
      if not isinstance(sample, BioSample):
        raise ValueError('ContainerSlot sample should be a Sample instance')
      # FIXME
      assert row >= 0 and column >= 0 and volume > 0
      columns = container.columns
      slot = row * columns + column
      assert slot < container.slots
      ome_obj = ome_type()
      super(PlateWell, self).__setup__(ome_obj, sample, container, slot)
      ome_obj.volume =  ort.rfloat(volume)
    super(PlateWell, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.volume is None:
      raise kb.KBError("PlateWell volume can't be None")
    else:
      super(PlateWell, self).__handle_validation_errors__()

  def __getattr__(self, name):
    if name == 'container':
      return TiterPlate(getattr(self.ome_obj, name))
    elif name == 'row':
      return self.slotPosition / self.container.columns
    elif name == 'column':
      return self.slotPosition % self.container.columns
    else:
      return super(PlateWell, self).__getattr__(name)

#------------------------------------------------------------
class DataSample(Sample, kb.DataSample):

  OME_TABLE = "DataSample"

  def __init__(self, from_=None, name=None, data_type=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      if name is None or data_type is None:
        raise ValueError('DataSample name and data_type cannot be None')
      ome_obj = ome_type()
      ome_obj.name = ort.rstring(name)
      ome_obj.dataType = data_type
      self.__setup__(ome_obj)
    super(DataSample, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.name is None:
      raise kb.KBError("DataSample name can't be None")
    else:
      super(DataSample, self).__handle_validation_errors__()

#------------------------------------------------------------
class BioSample(Sample, kb.BioSample):

  OME_TABLE = "BioSample"

  def __setattr__(self, name, value):
    if name == 'status':
      return setattr(self.ome_obj, name, value)
    else:
      return super(BioSample, self).__setattr__(name, value)

  def __handle_validation_errors__(self):
    if self.barcode is None:
      raise kb.KBError("BioSample barcode can't be None")
    elif self.initialVolume is None:
      raise kb.KBError("BioSample initialVolume can't be None")
    elif self.currentVolume is None:
      raise kb.KBError("BioSample currentVolume can't be None")
    elif self.status is None:
      raise kb.KBError("BioSample status can't be None")
    else:
      super(BioSample, self).__handle_validation_errors__()


#------------------------------------------------------------
class BloodSample(BioSample, kb.BloodSample):

  OME_TABLE = "BloodSample"

#------------------------------------------------------------
class DNASample(BioSample, kb.DNASample):

  OME_TABLE = "DNASample"

  def __handle_validation_errors__(self):
    if self.nanodropConcentration is None:
      raise kb.KBError("DNASample nanodropConcentration can't be None")
    elif self.qp230260 is None:
      raise kb.KBError("DNASample qp230260 can't be None")
    elif self.qp230280 is None:
      raise kb.KBError("DNASample qp230280 can't be None")
    else:
      super(DNASample, self).__handle_validation_errors__()


#------------------------------------------------------------
class SerumSample(BioSample, kb.SerumSample):

  OME_TABLE = "SerumSample"

#-----------------------------------------------------------
class DataCollectionItem(OmeroWrapper, kb.DataCollectionItem):

  OME_TABLE = "DataCollectionItem"

  def __init__(self, from_=None, data_collection=None, data_sample=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      if data_sample is None or data_collection is None:
        raise ValueError('DataCollectionItem  data_sample and data_collection cannot be None')
      # FIXME
      ome_obj = ome_type()
      ome_obj.vid = ort.rstring(vlu.make_vid())
      ome_obj.dataSample= data_sample.ome_obj
      ome_obj.dataSet   = data_collection.ome_obj
      ome_obj.dataCollUK = vluo.make_unique_key(data_collection.id, data_sample.ome_obj)
    super(DataCollectionItem, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.dataSample is None:
      raise kb.KBError("DataCollectionItem dataSample can't be None")
    elif self.dataCollection is None:
      raise kb.KBError("DataCollectionItem dataCollection can't be None")
    else:
      raise kb.KBError("unkwon error")

  def __getattr__(self, name):
    if name == 'dataSample':
      return DataSample(getattr(self.ome_obj, name))
    elif name == 'dataCollection':
      return DataCollection(getattr(self.ome_obj, name))
    else:
      return super(DataCollectionItem, self).__getattr__(name)
