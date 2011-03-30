import time

import bl.lib.sample.kb as kb

from proxy_core import ProxyCore

from study  import Study
from device import Device
from action import Action, ActionSetup, Device
from action_derived import ActionOnSample, ActionOnSampleSlot, ActionOnContainer
from result import Result
from sample import Sample, DataSample, BioSample
from sample import ContainerSlot, PlateWell
from sample import BloodSample, DNASample, SerumSample
from samples_container import SamplesContainer, TiterPlate



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
  Result      = Result
  Sample      = Sample
  SamplesContainer = SamplesContainer
  TiterPlate  = TiterPlate
  ContainerSlot = ContainerSlot
  PlateWell   = PlateWell
  DataSample  = DataSample
  BioSample   = BioSample
  BloodSample = BloodSample
  DNASample   = DNASample
  SerumSample = SerumSample

  def get_study_by_label(self, value):
    """
    Return the study object labeled 'value' or None if nothing matches 'value'.
    """
    result = self.ome_operation("getQueryService", "findByString",
                                Study.OME_TABLE, "label", value)
    return None if result is None else Study(result)

  def get_action_type_table(self):
    res = self.ome_operation("getQueryService", "findAll", "ActionType", None)
    return dict([(x._value._val, x) for x in res])

  def get_result_outcome_table(self):
    res = self.ome_operation("getQueryService", "findAll", "ResultOutcome", None)
    return dict([(x._value._val, x) for x in res])

  def get_sample_status_table(self):
    res = self.ome_operation("getQueryService", "findAll", "SampleStatus", None)
    return dict([(x._value._val, x) for x in res])


