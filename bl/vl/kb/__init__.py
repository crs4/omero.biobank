# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
The Knowledge Base (KB) package provides interfaces for the
application-side view of the backend object model, plus some syntact
sugar to simplify common operations.
"""

import sys


driver_table = {
  'omero': 'bl.vl.kb.drivers.omero',
  }


def KnowledgeBase(driver):
  try:
    __import__(driver_table[driver])
    driver_module = sys.modules[driver_table[driver]]
  except KeyError, e:
    raise ValueError('Driver %s is unknown' % driver)
  return driver_module.driver


class KBError(Exception):
  pass


class Study(object):
  def __init__(self):
    raise NotImplementedError


class Device(object):
  def __init__(self):
    raise NotImplementedError


class ActionSetup(object):
  def __init__(self):
    raise NotImplementedError


class Action(object):
  def __init__(self):
    raise NotImplementedError


class ActionOnVessel(Action):
  def __init__(self):
    raise NotImplementedError


class ActionOnDataSample(Action):
  def __init__(self):
    raise NotImplementedError


class ActionOnDataCollectionItem(Action):
  def __init__(self):
    raise NotImplementedError


class ActionOnAction(Action):
  def __init__(self):
    raise NotImplementedError


class Vessel(object):
  def __init__(self):
    raise NotImplementedError


class Tube(Vessel):
  def __init__(self):
    raise NotImplementedError


class PlateWell(Vessel):
  def __init__(self):
    raise NotImplementedError


class DataSample(object):
  def __init__(self):
    raise NotImplementedError


class DataObject(object):
  def __init__(self):
    raise NotImplementedError


class GenotypingMeasure(DataSample):
  def __init__(self):
    raise NotImplementedError


class AffymetrixCel(GenotypingMeasure):
  def __init__(self):
    raise NotImplementedError


class Collection(object):
  def __init__(self):
    raise NotImplementedError


class Container(Collection):
  def __init__(self):
    raise NotImplementedError


class SlottedContainer(Container):
  def __init__(self):
    raise NotImplementedError


class TiterPlate(SlottedContainer):
  def __init__(self):
    raise NotImplementedError


class DataCollection(Collection):
  def __init__(self):
    raise NotImplementedError


class DataCollectionItem(object):
  def __init__(self):
    raise NotImplementedError
