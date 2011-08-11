import wrapper as wp

from action import Action

from utils import assign_vid_and_timestamp

from data_samples import DataSample

class VLCollection(wp.OmeroWrapper):
  OME_TABLE = 'VLCollection'
  __fields__ = [('vid', wp.STRING, wp.REQUIRED),
                ('label', wp.STRING, wp.REQUIRED),
                ('creationDate', wp.TIMESTAMP, wp.REQUIRED),
                ('action', Action, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    return assign_vid_and_timestamp(conf, time_stamp_field='creationDate')

class ContainerStatus(wp.OmeroWrapper):
  OME_TABLE = 'ContainerStatus'
  __enums__ = ["INSTOCK", "UNUSABLE", "UNKNOWN", "INPREPARATION",
               "READY", "DISCARDED", "USED"]

class Container(VLCollection):
  OME_TABLE = 'Container'
  __fields__ = [('barcode', wp.STRING, wp.OPTIONAL),
                ('status',  ContainerStatus, wp.REQUIRED)
                ]

class SlottedContainer(Container):
  OME_TABLE = 'SlottedContainer'
  __fields__ = [('numberOfSlots', wp.INT, wp.REQUIRED),
                ('barcode', wp.STRING, wp.OPTIONAL)]

class TiterPlate(SlottedContainer):
  OME_TABLE = 'TiterPlate'
  __fields__ = [('rows', wp.INT, wp.REQUIRED),
                ('columns', wp.INT, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    if not 'numberOfSlots' in conf:
      conf['numberOfSlots'] = conf['rows'] * conf['columns']
    return super(TiterPlate, self).__preprocess_conf__(conf)

class DataCollection(VLCollection):
  OME_TABLE = 'DataCollection'
  __fields__ = []

class DataCollectionItem(wp.OmeroWrapper):
  OME_TABLE = 'DataCollectionItem'
  __fields__ = [('dataSample', DataSample, wp.REQUIRED),
                ('dataCollection', DataCollection, wp.REQUIRED)]


