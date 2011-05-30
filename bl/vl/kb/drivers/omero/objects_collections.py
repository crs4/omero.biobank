import wrapper as wp

from action import Action
from data_samples import DataSample

class VLCollection(wp.OmeroWrapper):
  OME_TABLE = 'VLCollection'
  __fields__ = [('vid', wp.string, wp.required),
                ('label', wp.string, wp.required),
                ('creationDate', wp.timestamp, wp.required),
                ('action', Action, wp.required)]


class Container(VLCollection):
  OME_TABLE = 'Container'
  __fields__ = [('barcode', wp.string, wp.optional)]

class SlottedContainer(Container):
  OME_TABLE = 'SlottedContainer'
  __fields__ = [('numberOfSlots', wp.int, wp.required),
                ('barcode', wp.string, wp.optional)]

class TiterPlate(SlottedContainer):
  OME_TABLE = 'TiterPlate'
  __fields__ = [('rows', wp.int, wp.required),
                ('columns', wp.int, wp.required)]

class DataCollection(VLCollection):
  OME_TABLE = 'DataCollection'
  __fields__ = []

class DataCollectionItem(wp.OmeroWrapper):
  OME_TABLE = 'DataCollectionItem'
  __fields__ = [('dataSample', DataSample, wp.required),
                ('dataCollection', Datacollection, wp.required)]


