from wrapper import OmeroWrapper

import omero.rtypes as ort

import bl.vl.utils           as vlu
import bl.vl.utils.ome_utils as vluo

import bl.vl.sample.kb as kb

from sample import Sample
from sample import SamplesContainerSlot, DataCollectionItem
from samples_container import SamplesContainer, DataCollection
from study  import Study
from action import Action

import time

#----------------------------------------------------------------------
class ActionOnSample(Action, kb.ActionOnSample):

  OME_TABLE = "ActionOnSample"

  def __handle_validation_errors__(self):
    if self.target is None:
      raise kb.KBError("ActionOnSample target can't be None")
    else:
      super(ActionOnSample, self).__handle_validation_errors__()

  def __setattr__(self, name, value):
    if name == 'target':
      if not isinstance(value, Sample):
        raise ValueError('ActionOnSample target should be a Sample instance')
      return setattr(self.ome_obj, name, value.ome_obj)
    else:
      return super(ActionOnSample, self).__setattr__(name, value)

  def __getattr__(self, name):
    if name == 'target':
      return Sample(self.ome_obj.__getattr__(name), proxy=self.proxy)
    else:
      return super(ActionOnSample, self).__getattr__(name)


#----------------------------------------------------------------------
class ActionOnSamplesContainer(Action, kb.ActionOnSamplesContainer):

  OME_TABLE = "ActionOnSamplesContainer"

  def __handle_validation_errors__(self):
    if self.target is None:
      raise kb.KBError("ActionOnSamplesContainer target can't be None")
    else:
      super(ActionOnSamplesContainer, self).__handle_validation_errors__()

  def __setattr__(self, name, value):
    if name == 'target':
      if not isinstance(value, SamplesContainer):
        raise ValueError('ActionOnSamplesContainer target should be a SamplesContainer instance')
      return setattr(self.ome_obj, name, value.ome_obj)
    else:
      return super(ActionOnSamplesContainer, self).__setattr__(name, value)

  def __getattr__(self, name):
    if name == 'target':
      return SamplesContainer(self.ome_obj.__getattr__(name), proxy=self.proxy)
    else:
      return super(ActionOnSamplesContainer, self).__getattr__(name)

#----------------------------------------------------------------------
class ActionOnSamplesContainerSlot(Action, kb.ActionOnSamplesContainerSlot):

  OME_TABLE = "ActionOnSamplesContainerSlot"

  def __handle_validation_errors__(self):
    if self.target is None:
      raise kb.KBError("ActionOnSamplesContainerSlot target can't be None")
    else:
      super(ActionOnSamplesContainerSlot, self).__handle_validation_errors__()

  def __setattr__(self, name, value):
    if name == 'target':
      if not isinstance(value, SamplesContainerSlot):
        raise ValueError('ActionOnSamplesContainerSlot target should be a SamplesContainerSlot instance')
      return setattr(self.ome_obj, name, value.ome_obj)
    else:
      return super(ActionOnSamplesContainerSlot, self).__setattr__(name, value)

  def __getattr__(self, name):
    if name == 'target':
      return SamplesContainerSlot(self.ome_obj.__getattr__(name),
                                  proxy=self.proxy)
    else:
      return super(ActionOnSamplesContainerSlot, self).__getattr__(name)


#----------------------------------------------------------------------
class ActionOnDataCollection(Action, kb.ActionOnDataCollection):

  OME_TABLE = "ActionOnDataCollection"

  def __handle_validation_errors__(self):
    if self.target is None:
      raise kb.KBError("ActionOnDataCollection target can't be None")
    else:
      super(ActionOnDataCollection, self).__handle_validation_errors__()

  def __setattr__(self, name, value):
    if name == 'target':
      if not isinstance(value, DataCollection):
        raise ValueError('ActionOnDataCollection target should be a DataCollection instance')
      return setattr(self.ome_obj, name, value.ome_obj)
    else:
      return super(ActionOnDataCollection, self).__setattr__(name, value)

  def __getattr__(self, name):
    if name == 'target':
      return DataCollection(self.ome_obj.__getattr__(name), proxy=self.proxy)
    else:
      return super(ActionOnDataCollection, self).__getattr__(name)


#----------------------------------------------------------------------
class ActionOnDataCollectionItem(Action, kb.ActionOnDataCollectionItem):

  OME_TABLE = "ActionOnDataCollectionItem"

  def __handle_validation_errors__(self):
    if self.target is None:
      raise kb.KBError("ActionOnDataCollectionItem target can't be None")
    else:
      super(ActionOnDataCollectionItem, self).__handle_validation_errors__()

  def __setattr__(self, name, value):
    if name == 'target':
      if not isinstance(value, DataCollectionItem):
        raise ValueError('ActionOnDataCollectionItem target should be a DataCollectionItem instance')
      return setattr(self.ome_obj, name, value.ome_obj)
    else:
      return super(ActionOnDataCollectionItem, self).__setattr__(name, value)

  def __getattr__(self, name):
    if name == 'target':
      return DataCollectionItem(self.ome_obj.__getattr__(name),
                                proxy=self.proxy)
    else:
      return super(ActionOnDataCollectionItem, self).__getattr__(name)
