# BEGIN_COPYRIGHT
# END_COPYRIGHT

import wrapper as wp
from action import Action
from utils import assign_vid_and_timestamp, assign_vid, make_unique_key
from data_samples import DataSample, Reads


class VLCollection(wp.OmeroWrapper):
  
  OME_TABLE = 'VLCollection'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('label', wp.STRING, wp.REQUIRED),
                ('creationDate', wp.TIMESTAMP, wp.REQUIRED),
                ('action', Action, wp.REQUIRED),
                ('lastUpdate', Action, wp.OPTIONAL)]

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


class FlowCell(SlottedContainer):

  OME_TABLE = 'FlowCell'
  __fields__ = []


class Lane(Container):
  OME_TABLE = 'Lane'
  __fields__ = [('flowCell', FlowCell, wp.REQUIRED),
                ('slot', wp.INT, wp.REQUIRED)]

class LaneSlot(wp.OmeroWrapper):

  OME_TABLE = 'LaneSlot'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('lane', Lane, wp.REQUIRED),
                ('tag', wp.STRING, wp.OPTIONAL),
                ('laneSlotUK', wp.STRING, wp.OPTIONAL),
                ('action', Action, wp.REQUIRED),
                ('lastUpdate', Action, wp.OPTIONAL)]

  def __preprocess_conf__(self, conf):
    if not 'laneSlotUK' in conf and 'tag' in conf:
      conf['laneSlotUK'] = make_unique_key(conf['tag'], conf['lane'].label)
    return assing_vid(conf)

  def __update_constraints__(self):
    if self.tag:
      ls_uk = make_unique_key(self.tag, self.lane.label)
      setattr(self.ome_obj, 'laneSlotUK',
              self.to_omero(self.__field__['laneSlotUK'][0], ls_uk))


class DataCollection(VLCollection):
  
  OME_TABLE = 'DataCollection'
  __fields__ = []


class SequencerOutput(DataCollection):

  OME_TABLE = 'SequencerOutput'
  __fields__ = [('read1', Reads, wp.REQUIRED),
                ('read2', Reads, wp.OPTIONAL),
                ('read3', Reads, wp.OPTIONAL)]


class DataCollectionItem(wp.OmeroWrapper):
  
  OME_TABLE = 'DataCollectionItem'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('dataSample', DataSample, wp.REQUIRED),
                ('dataCollection', DataCollection, wp.REQUIRED),
                ('dataCollectionItemUK', wp.STRING, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    if not 'dataCollectionItemUK' in conf:
      dc_vid = conf['dataCollection'].id
      ds_vid = conf['dataSample'].id
      conf['dataCollectionItemUK'] = make_unique_key(dc_vid, ds_vid)
    return assign_vid(conf)

  def __update_constraints__(self):
    dci_uk = make_unique_key(self.dataCollection.id, self.dataSample.id)
    setattr(self.ome_obj, 'dataCollectionItemUK',
            self.to_omero(self.__fields__['dataCollectionItemUK'][0], dci_uk))
