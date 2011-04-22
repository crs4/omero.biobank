import time

import bl.vl.sample.kb as kb

from proxy_indexed import ProxyIndexed

from study  import Study
from device import Device
from action import Action, ActionSetup
from action_derived import ActionOnSample
from action_derived import ActionOnSamplesContainerSlot, ActionOnSamplesContainer
from action_derived import ActionOnDataCollection, ActionOnDataCollectionItem
from result import Result
from sample import Sample, DataSample, BioSample
from sample import SamplesContainerSlot, PlateWell
from sample import BloodSample, DNASample, SerumSample
from sample import DataCollection, DataCollectionItem
from samples_container import SamplesContainer, TiterPlate
from data_object import DataObject

from genotyping import AffymetrixCel, SNPMarkersSet, GenotypeDataSample


import logging

logger = logging.getLogger()

class Proxy(ProxyIndexed):
  """
  A knowledge base for the Sample package implemented as a driver for
  OMERO.
  """
  Study       = Study
  Device      = Device
  Action      = Action
  ActionSetup = ActionSetup
  ActionOnSample = ActionOnSample
  ActionOnSamplesContainerSlot = ActionOnSamplesContainerSlot
  ActionOnSamplesContainer = ActionOnSamplesContainer
  ActionOnDataCollection = ActionOnDataCollection
  ActionOnDataCollectionItem = ActionOnDataCollectionItem
  #-
  Result      = Result
  Sample      = Sample
  SamplesContainer = SamplesContainer
  TiterPlate  = TiterPlate
  SamplesContainerSlot = SamplesContainerSlot
  PlateWell   = PlateWell
  DataSample  = DataSample
  DataCollection  = DataCollection
  DataCollectionItem  = DataCollectionItem
  DataObject  = DataObject
  BioSample   = BioSample
  BloodSample = BloodSample
  DNASample   = DNASample
  SerumSample = SerumSample
  #-
  AffymetrixCel = AffymetrixCel
  SNPMarkersSet = SNPMarkersSet
  GenotypeDataSample = GenotypeDataSample

  ProxyIndexed.INDEXED_TARGET_TYPES.extend([Result])

  def get_all_instances(self, klass):
    table_name = klass.get_ome_table()
    res = self.ome_operation("getQueryService", "findAll", table_name, None)
    return [klass(x) for x in res]


  def get_action_category_table(self):
    res = self.ome_operation("getQueryService", "findAll", "ActionCategory", None)
    return dict([(x._value._val, x) for x in res])

  def get_result_outcome_table(self):
    res = self.ome_operation("getQueryService", "findAll", "ResultOutcome", None)
    return dict([(x._value._val, x) for x in res])

  def get_sample_status_table(self):
    res = self.ome_operation("getQueryService", "findAll", "SampleStatus", None)
    return dict([(x._value._val, x) for x in res])

  def get_data_type_table(self):
    res = self.ome_operation("getQueryService", "findAll", "DataType", None)
    return dict([(x._value._val, x) for x in res])

  def get_study_by_label(self, value):
    """
    Return the study object labeled 'value' or None if nothing matches 'value'.
    """
    query = 'select st from Study st where st.label = :label'
    pars = self.ome_query_params({'label' : self.ome_wrap(value, 'string')})
    result = self.ome_operation("getQueryService", "findByQuery", query, pars)
    return None if result is None else Study(result)

  def get_action_setup_by_label(self, label):
    """
    Return the ActionSetup object labeled 'label' or None if nothing matches 'label'.
    """
    query = 'select a from ActionSetup a where a.label = :label'
    pars = self.ome_query_params({'label' : self.ome_wrap(label, 'string')})
    result = self.ome_operation("getQueryService", "findByQuery", query, pars)
    return None if result is None else ActionSetup(result)

  def get_device(self, label):
    """
    """
    query = 'select d from Device d where d.label = :label'
    pars = self.ome_query_params({'label' : self.ome_wrap(label)})
    result = self.ome_operation("getQueryService", "findByQuery", query, pars)
    return None if result is None else Device(result)

  def get_snp_markers_set(self, maker, model, release):
    """
    """
    query = """select ms from SNPMarkersSet ms
               where ms.maker = :maker and ms.model = :model and ms.release = :release
               """
    pars = self.ome_query_params({'maker' : self.ome_wrap(maker),
                                  'model' : self.ome_wrap(model),
                                  'release' : self.ome_wrap(release)
                                  })
    result = self.ome_operation("getQueryService", "findByQuery", query, pars)
    return None if result is None else SNPMarkersSet(result)

  def get_devices(self):
    """
    """
    return self.get_all_instances(Device)

  def get_titer_plates(self, filter=None):
    result = self.ome_operation("getQueryService", "findAll", "TiterPlate", None)
    return [TiterPlate(r) for r in result]

  def get_wells_of_plate(self, plate):
    query = 'select w from PlateWell w join w.container as c where c.vid = :c_id'
    pars = self.ome_query_params({'c_id' : self.ome_wrap(plate.id)})
    result = self.ome_operation("getQueryService", "findAllByQuery", query, pars)
    logger.debug('get_wells_of_plate results: %s' % result)
    return [PlateWell(r) for r in result]

  def get_data_collection_items(self, data_collection):
    query = 'select dci from DataCollectionItem dci join dci.dataSet as c where c.vid = :c_id'
    pars = self.ome_query_params({'c_id' : self.ome_wrap(data_collection.id)})
    result = self.ome_operation("getQueryService", "findAllByQuery", query, pars)
    logger.debug('get_data_collection_items results:[%d] %s' % (len(result), result))
    return [DataCollectionItem(r) for r in result]

  def get_bio_sample(self, aklass, barcode):
    query = 'select s from %s s where s.barcode = :barcode' % aklass.OME_TABLE
    pars = self.ome_query_params({'barcode' : self.ome_wrap(barcode, 'string')})
    result = self.ome_operation("getQueryService", "findByQuery", query, pars)
    return None if result is None else aklass(result)

  def get_blood_sample(self, barcode):
    """
    Get a BloodSample object stored in VL.
    """
    return self.get_bio_sample(BloodSample, barcode)

  def get_dna_sample(self, barcode):
    """
    Get a DNAsample object stored in VL.
    """
    return self.get_bio_sample(DNASample, barcode)

  def get_titer_plate(self, barcode):
    """
    Get a TiterPlate object stored in VL.
    """
    #FIXME this should have a more reasonable name...
    return self.get_bio_sample(TiterPlate, barcode)
