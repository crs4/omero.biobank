import omero.model as om
import omero.rtypes as ort

import time

from utils import assign_vid_and_timestamp

import wrapper as wp

class Study(wp.OmeroWrapper):
  OME_TABLE = 'Study'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('label', wp.STRING, wp.REQUIRED),
                ('startDate', wp.TIMESTAMP, wp.REQUIRED),
                ('endDate', wp.TIMESTAMP, wp.OPTIONAL),
                ('description', wp.STRING, wp.OPTIONAL)]

  def __preprocess_conf__(self, conf):
    return assign_vid_and_timestamp(conf, time_stamp_field='startDate')



class Device(wp.OmeroWrapper):
  OME_TABLE = 'Device'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('label', wp.STRING, wp.REQUIRED),
                ('maker', wp.STRING, wp.REQUIRED),
                ('model', wp.STRING, wp.REQUIRED),
                ('release', wp.STRING, wp.REQUIRED)]

class HardwareDevice(Device):
  OME_TABLE = 'HardwareDevice'
  __fields__ = [('barcode', wp.STRING, wp.OPTIONAL),
                ('physicalLocation', wp.STRING, wp.OPTIONAL)]

class ActionCategory(wp.OmeroWrapper):
  OME_TABLE = 'ActionCategory'
  __enums__ = ['IMPORT', 'CREATION', 'EXTRACTION', 'UPDATE',
               'ALIQUOTING', 'MEASUREMENT', 'PROCESSING']

class ActionSetup(wp.OmeroWrapper):
  OME_TABLE = 'ActionSetup'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('label', wp.STRING, wp.REQUIRED),
                ('conf', wp.STRING, wp.REQUIRED)]


class Action(wp.OmeroWrapper):
  OME_TABLE = 'Action'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('beginTime', wp.TIMESTAMP, wp.REQUIRED),
                ('endTime', wp.TIMESTAMP, wp.OPTIONAL),
                ('setup', ActionSetup, wp.OPTIONAL),
                ('device', Device, wp.OPTIONAL),
                ('actionCategory', ActionCategory, wp.REQUIRED),
                ('operator', wp.STRING, wp.REQUIRED),
                ('context', Study, wp.REQUIRED),
                ('description', wp.TEXT, wp.OPTIONAL)]

  def __preprocess_conf__(self, conf):
    return assign_vid_and_timestamp(conf, time_stamp_field='beginTime')
