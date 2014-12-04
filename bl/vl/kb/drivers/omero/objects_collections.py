# BEGIN_COPYRIGHT
# END_COPYRIGHT

import wrapper as wp
from action import Action
from utils import assign_vid_and_timestamp, assign_vid, make_unique_key
from data_samples import DataSample


class VLCollection(wp.OmeroWrapper):

  OME_TABLE = 'VLCollection'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('label', wp.STRING, wp.REQUIRED),
                ('creationDate', wp.TIMESTAMP, wp.REQUIRED),
                ('action', Action, wp.REQUIRED),
                ('lastUpdate', Action, wp.OPTIONAL),
                ('labelUK', wp.STRING, wp.REQUIRED)]
  __do_not_serialize__ = ['labelUK']

  def __preprocess_conf__(self, conf):
    if not 'labelUK' in conf:
      conf['labelUK'] = make_unique_key(self.get_namespace(), conf['label'])
    return assign_vid_and_timestamp(conf, time_stamp_field='creationDate')

  def __update_constraints__(self):
    l_uk = make_unique_key(self.get_namespace(), self.label)
    setattr(self.ome_obj, 'labelUK',
            self.to_omero(self.__fields__['labelUK'][0], l_uk))


class ContainerStatus(wp.OmeroWrapper):

  OME_TABLE = 'ContainerStatus'
  __enums__ = ["INSTOCK", "UNUSABLE", "UNKNOWN", "INPREPARATION",
               "READY", "DISCARDED", "USED"]


class Container(VLCollection):

  OME_TABLE = 'Container'
  __fields__ = [('barcode', wp.STRING, wp.OPTIONAL),
                ('status',  ContainerStatus, wp.REQUIRED),
                ('barcodeUK', wp.STRING, wp.OPTIONAL)]
  __do_not_serialize__ = ['barcodeUK'] + VLCollection.__do_not_serialize__

  def __preprocess_conf__(self, conf):
    if not 'barcodeUK' in conf and conf.get('barcode'):
      conf['barcodeUK'] = make_unique_key(self.get_namespace(), conf['barcode'])
    return super(Container, self).__preprocess_conf__(conf)

  def __update_constraints__(self):
    self.__fields__['labelUK'] = super(Container, self).__fields__['labelUK']
    if self.barcode:
      b_uk = make_unique_key(self.get_namespace(), self.barcode)
      setattr(self.ome_obj, 'barcodeUK',
              self.to_omero(self.__fields__['barcodeUK'][0], b_uk))
    super(Container, self).__update_constraints__()


class SlottedContainer(Container):

  OME_TABLE = 'SlottedContainer'
  __fields__ = [('numberOfSlots', wp.INT, wp.REQUIRED)]

  def __update_constraints__(self):
    self.__fields__['barcodeUK'] = super(SlottedContainer, self).__fields__['barcodeUK']
    super(SlottedContainer, self).__update_constraints__()


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
                ('slot', wp.INT, wp.REQUIRED),
                ('laneUK', wp.STRING, wp.REQUIRED)]
  __do_not_serialize__ = ['laneUK']

  def __preprocess_conf__(self, conf):
    if not 'label' in conf:
      conf['label'] = '%s:%s' % (conf['flowCell'].label, conf['slot'])
    if not 'laneUK' in conf:
      conf['laneUK'] = make_unique_key(conf['flowCell'].label, conf['slot'])
    return super(Lane, self).__preprocess_conf__(conf)

  def __update_constraints__(self):
    self.__fields__['barcodeUK'] = super(Lane, self).__fields__['barcodeUK']
    l_uk = make_unique_key(self.flowCell.label, self.slot)
    setattr(self.ome_obj, 'laneUK',
            self.to_omero(self.__fields__['laneUK'][0], l_uk))
    super(Lane, self).__update_constraints__()

  def __dump_to_graph__(self, is_update):
    super(Lane, self).__dump_to_graph__(is_update)
    self.proxy.dt.create_collection_item(self, self.flowCell)


class DataCollection(VLCollection):

  OME_TABLE = 'DataCollection'
  __fields__ = []

  def __update_constraints__(self):
    self.__fields__['labelUK'] = super(DataCollection, self).__fields__['labelUK']
    super(DataCollection, self).__update_constraints__()


class DataCollectionItem(wp.OmeroWrapper):

  OME_TABLE = 'DataCollectionItem'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('dataSample', DataSample, wp.REQUIRED),
                ('dataCollection', DataCollection, wp.REQUIRED),
                ('dataCollectionItemUK', wp.STRING, wp.REQUIRED)]
  __do_not_serialize__ = ['dataCollectionItemUK']

  def __preprocess_conf__(self, conf):
    if not 'dataCollectionItemUK' in conf:
      dc_vid = conf['dataCollection'].id
      ds_vid = conf['dataSample'].id
      conf['dataCollectionItemUK'] = make_unique_key(self.get_namespace(),
                                                     dc_vid, ds_vid)
    return assign_vid(conf)

  def __update_constraints__(self):
    dci_uk = make_unique_key(self.get_namespace(), self.dataCollection.id,
                             self.dataSample.id)
    setattr(self.ome_obj, 'dataCollectionItemUK',
            self.to_omero(self.__fields__['dataCollectionItemUK'][0], dci_uk))

  def __dump_to_graph__(self, is_update):
    super(DataCollectionItem, self).__dump_to_graph__(is_update)
    self.proxy.dt.create_collection_item(self.dataSample,
                                         self.dataCollection)


class TaggedDataCollectionItem(DataCollectionItem):

  OME_TABLE = "TaggedDataCollectionItem"
  __fields__ = [('role', wp.STRING, wp.REQUIRED)]

  def __update_constraints__(self):
    self.__fields__['dataCollectionItemUK'] = super(TaggedDataCollectionItem,
                                                    self).__fields__['dataCollectionItemUK']
    super(TaggedDataCollectionItem, self).__update_constraints__()
