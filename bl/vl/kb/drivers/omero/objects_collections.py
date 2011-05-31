import wrapper as wp

from action import Action, assing_vid_and_timestamp

from data_samples import DataSample

class VLCollection(wp.OmeroWrapper):
  OME_TABLE = 'VLCollection'
  __fields__ = [('vid', wp.STRING, wp.REQUIRED),
                ('label', wp.STRING, wp.REQUIRED),
                ('creationDate', wp.TIMESTAMP, wp.REQUIRED),
                ('action', Action, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    return assing_vid_and_timestamp(conf, time_stamp_field='creationDate')

class Container(VLCollection):
  OME_TABLE = 'Container'
  __fields__ = [('barcode', wp.STRING, wp.OPTIONAL)]

class SlottedContainer(Container):
  OME_TABLE = 'SlottedContainer'
  __fields__ = [('numberOfSlots', wp.INT, wp.REQUIRED),
                ('barcode', wp.STRING, wp.OPTIONAL)]

class TiterPlate(SlottedContainer):
  OME_TABLE = 'TiterPlate'
  __fields__ = [('rows', wp.INT, wp.REQUIRED),
                ('columns', wp.INT, wp.REQUIRED)]

class DataCollection(VLCollection):
  OME_TABLE = 'DataCollection'
  __fields__ = []

class DataCollectionItem(wp.OmeroWrapper):
  OME_TABLE = 'DataCollectionItem'
  __fields__ = [('dataSample', DataSample, wp.REQUIRED),
                ('dataCollection', DataCollection, wp.REQUIRED)]


