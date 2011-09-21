"""
This module provides the KnowledgeBase entry point, interfaces for the
application-side view of the object model that should be implemented
by the kb drivers, and some syntact sugar to simplify common
operations.
"""

import sys
#--
#
# The actual KnowledgeBase front-end
#
#--
driver_table = { 'omero' : 'bl.vl.kb.drivers.omero' }

def KnowledgeBase(driver):
  try:
    __import__(driver_table[driver])
    driver_module = sys.modules[driver_table[driver]]
  except KeyError, e:
    raise ValueError('Driver %s is unknown' % driver)
  return driver_module.driver

def Individuals(group, kb=None):
  """
  Syntactic sugar to simplify the looping on individuals contained in a group.
  The idea is that it should be possible to do the following:

  .. code-block:: python

    for i in Individuals(group, kb=kb):
      for d in DataSamples(i, dsample_name, kb=kb):
        gds = filter(lambda x: x.snpMarkersSet == mset)


  :param group: a study object, we will be looping on all the
                Individual enrolled in it.
  :type group: Study

  :type return: generator

  """
  return (e in kb.get_enrolled(group))

def DataSamples()

    def DataSamples(individual, data_sample_klass_name='DataSample'):
      klass = getattr(self.kb, data_sample_klass_name)
      return dt.get_connected(individual, aklass=klass)


#--
#
# Interface definitions
#
#--

class KBError(Exception):
  pass

#--
class Study(object):

  def __init__(self):
    raise NotImplementedError

#--
class Device(object):

  def __init__(self):
    raise NotImplementedError

#--
class ActionSetup(object):

  def __init__(self):
    raise NotImplementedError


#--
class Action(object):

  def __init__(self):
    raise NotImplementedError

#--
class ActionOnVessel(Action):

  def __init__(self):
    raise NotImplementedError

#--
class ActionOnDataSample(Action):

  def __init__(self):
    raise NotImplementedError

#--
class ActionOnDataCollectionItem(Action):

  def __init__(self):
    raise NotImplementedError

#--
class ActionOnAction(Action):

  def __init__(self):
    raise NotImplementedError

#--
class Vessel(object):

  def __init__(self):
    raise NotImplementedError

#--
class Tube(Vessel):

  def __init__(self):
    raise NotImplementedError

#--
class PlateWell(Vessel):

  def __init__(self):
    raise NotImplementedError

#--
class DataSample(object):

  def __init__(self):
    raise NotImplementedError

#--
class DataObject(object):

  def __init__(self):
    raise NotImplementedError

#--
class GenotypingMeasure(DataSample):

  def __init__(self):
    raise NotImplementedError

#--
class AffymetrixCel(GenotypingMeasure):

  def __init__(self):
    raise NotImplementedError


#--
class Collection(object):

  def __init__(self):
    raise NotImplementedError

#--
class Container(Collection):

  def __init__(self):
    raise NotImplementedError

#--
class SlottedContainer(Container):

  def __init__(self):
    raise NotImplementedError

#--
class TiterPlate(SlottedContainer):

  def __init__(self):
    raise NotImplementedError

#--
class DataCollection(Collection):

  def __init__(self):
    raise NotImplementedError


#--
class DataCollectionItem(object):

  def __init__(self):
    raise NotImplementedError


