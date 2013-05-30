# BEGIN_COPYRIGHT
# END_COPYRIGHT

from action import Action
from vessels import Vessel
from data_samples import DataSample
from objects_collections import VLCollection, DataCollectionItem


class ActionOnVessel(Action):
  
  OME_TABLE = 'ActionOnVessel'
  __fields__ = [('target', Vessel, 'required')]


class ActionOnDataSample(Action):
  
  OME_TABLE = 'ActionOnDataSample'
  __fields__ = [('target', DataSample, 'required')]


class ActionOnDataCollectionItem(Action):
  
  OME_TABLE = 'ActionOnDataCollectionItem'
  __fields__ = [('target', DataCollectionItem, 'required')]


class ActionOnAction(Action):
  
  OME_TABLE = 'ActionOnAction'
  __fields__ = [('target', Action, 'required')]

  def __cleanup__(self):
    pass


class ActionOnCollection(Action):

  OME_TABLE = 'ActionOnCollection'
  __fields__ = [('target', VLCollection, 'required')]
