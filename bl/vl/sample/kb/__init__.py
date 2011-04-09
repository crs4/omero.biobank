"""
Interfaces for the application-side view of the object model.

Sample kb drivers should implement these interfaces.
"""

import sys
#--
#
# The actual KnowledgeBase front-end
#
#--
driver_table = { 'omero' : 'bl.vl.sample.kb.drivers.omero' }

def KnowledgeBase(driver):
  try:
    __import__(driver_table[driver])
    driver_module = sys.modules[driver_table[driver]]
  except KeyError, e:
    print 'Driver %s is unknown' % driver
    assert(False)
  return driver_module.driver

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
class DataObject(object):

  def __init__(self):
    raise NotImplementedError

#--
class Action(object):

  def __init__(self):
    raise NotImplementedError

#--
class ActionOnSample(object):

  def __init__(self):
    raise NotImplementedError

#--
class ActionOnSamplesContainer(object):

  def __init__(self):
    raise NotImplementedError

#--
class ActionOnSamplesContainerSlot(object):

  def __init__(self):
    raise NotImplementedError

#--
class ActionOnDataCollection(object):

  def __init__(self):
    raise NotImplementedError

#--
class ActionOnDataCollectionItem(object):

  def __init__(self):
    raise NotImplementedError

#--
class ActionSetup(object):

  def __init__(self):
    raise NotImplementedError

#--
class Device(object):

  def __init__(self):
    raise NotImplementedError

#--
class Result(object):

  def __init__(self):
    raise NotImplementedError

#--
class Sample(object):

  def __init__(self):
    raise NotImplementedError

#--
class SamplesContainerSlot(object):

  def __init__(self):
    raise NotImplementedError

#--
class SamplesContainer(object):

  def __init__(self):
    raise NotImplementedError

#--
class TiterPlate(object):

  def __init__(self):
    raise NotImplementedError

#--
class PlateWell(object):

  def __init__(self):
    raise NotImplementedError

#--
class DataSample(object):

  def __init__(self):
    raise NotImplementedError


#--
class BioSample(object):

  def __init__(self):
    raise NotImplementedError

#--
class BloodSample(object):

  def __init__(self):
    raise NotImplementedError

#--
class DNASample(object):

  def __init__(self):
    raise NotImplementedError

#--
class SerumSample(object):

  def __init__(self):
    raise NotImplementedError

#--
class DataCollectionItem(object):

  def __init__(self):
    raise NotImplementedError

#--
class DataCollection(object):

  def __init__(self):
    raise NotImplementedError

#--
class DataObject(object):

  def __init__(self):
    raise NotImplementedError
