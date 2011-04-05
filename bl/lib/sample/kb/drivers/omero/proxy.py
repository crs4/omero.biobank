import time

import bl.lib.sample.kb as kb
import vl.lib.utils     as vlu

from proxy_core import ProxyCore

from study  import Study
from device import Device
from action import Action, ActionSetup
from action_derived import ActionOnSample, ActionOnSampleSlot, ActionOnContainer
from action_derived import ActionOnDataCollection, ActionOnDataCollectionItem
from result import Result
from sample import Sample, DataSample, BioSample
from sample import ContainerSlot, PlateWell
from sample import BloodSample, DNASample, SerumSample
from sample import DataCollection, DataCollectionItem
from samples_container import SamplesContainer, TiterPlate
from data_object import DataObject

class Proxy(ProxyCore):
  """
  A knowledge base for the Sample package implemented as a driver for
  OMERO.
  """
  Study       = Study
  Device      = Device
  Action      = Action
  ActionSetup = ActionSetup
  ActionOnSample = ActionOnSample
  ActionOnSampleSlot = ActionOnSampleSlot
  ActionOnContainer = ActionOnContainer
  ActionOnDataCollection = ActionOnDataCollection
  ActionOnDataCollectionItem = ActionOnDataCollectionItem
  Result      = Result
  Sample      = Sample
  SamplesContainer = SamplesContainer
  TiterPlate  = TiterPlate
  ContainerSlot = ContainerSlot
  PlateWell   = PlateWell
  DataSample  = DataSample
  DataCollection  = DataCollection
  DataCollectionItem  = DataCollectionItem
  DataObject  = DataObject
  BioSample   = BioSample
  BloodSample = BloodSample
  DNASample   = DNASample
  SerumSample = SerumSample

  ACTION_TABLE='vl_action_table.h5'
  ACTION_RESULT_CLASS_MAX_NAME_LEN = 256
  ACTION_TABLE_COLUMNS =  [('string', 'r_type', 'Result object type', ACTION_RESULT_CLASS_MAX_NAME_LEN, None),
                           ('string', 'r_vid',  'Result object VID',        len(vlu.make_vid()), None),
                           ('long',   'r_id',   'Result object ID',         None),
                           ('string', 'o_vid',  'Action VID',               len(vlu.make_vid()), None),
                           ('string', 't_vid',  'Action target object VID', len(vlu.make_vid()), None),
                           ('string', 'i_vid',  'Root tree VID',            len(vlu.make_vid()), None)]

  def __init__(self, host, user, passwd):
    super(Proxy, self).__init__(host, user, passwd)
    self.create_if_missing(self.ACTION_TABLE, self.ACTION_TABLE_COLUMNS)

  #---------------------------------------------------------------------------------------------
  def create_if_missing(self, table_name, fields):
    if not self.table_exists(table_name):
      self.create_table(table_name, fields)

  def get_objects_linked_to_object(self, object):
    data = self.get_table_rows(self.ACTION_TABLE, selector, batch_size)
    #[apply(Foo, ome_obj) for Foo, ome_obj in data]

  def get_actions_tree(self, vid):
    pass
  #----------------------------------------------------------------------------------------------

  def get_action_type_table(self):
    res = self.ome_operation("getQueryService", "findAll", "ActionType", None)
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

  def get_device(self, vendor, model, release):
    """
    """
    query = 'select d from Device d where d.vendor = :vendor and d.model = :model and d.release = :release'
    pars = self.ome_query_params({'vendor' : self.ome_wrap(vendor),
                                  'model'  : self.ome_wrap(model),
                                  'release' : self.ome_wrap(release)})
    result = self.ome_operation("getQueryService", "findByQuery", query, pars)
    return None if result is None else Device(result)

  def get_devices(self):
    """
    """
    res = self.ome_operation("getQueryService", "findAll", "Device", None)
    return [Device(x) for x in res]

  def get_titer_plates(self, filter=None):
    result = self.ome_operation("getQueryService", "findAll", "TiterPlate", None)
    return [TiterPlate(r) for r in result]

  def get_wells_of_plate(self, plate):
    query = 'select w from PlateWell w join w.container as c where c.vid = :c_id'
    pars = self.ome_query_params({'c_id' : self.ome_wrap(plate.id)})
    result = self.ome_operation("getQueryService", "findAllByQuery", query, pars)
    return [PlateWell(r) for r in result]





