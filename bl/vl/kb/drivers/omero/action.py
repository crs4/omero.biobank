# BEGIN_COPYRIGHT
# END_COPYRIGHT

import omero.model as om
import omero.rtypes as ort

from utils import assign_vid_and_timestamp
import wrapper as wp
from genotyping import SNPMarkersSet


class OriginalFile(wp.OmeroWrapper):

  # Mapping only fields used also to define DataObjects
  OME_TABLE = 'OriginalFile'
  __fields__ = [('name',     wp.STRING, wp.REQUIRED),
                ('path',     wp.STRING, wp.REQUIRED),
                ('mimetype', wp.STRING, wp.OPTIONAL),
                ('sha1',     wp.STRING, wp.REQUIRED),
                ('size',     wp.LONG,   wp.REQUIRED)]


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


class SoftwareProgram(Device):
  
  OME_TABLE = 'SoftwareProgram'
  __fields__ = []


class GenotypingProgram(SoftwareProgram):
  
  OME_TABLE = 'GenotypingProgram'
  __fields__ = [('snpMarkersSet', SNPMarkersSet, wp.REQUIRED)]


class HardwareDevice(Device):
  
  OME_TABLE = 'HardwareDevice'
  __fields__ = [('barcode', wp.STRING, wp.OPTIONAL),
                ('physicalLocation', wp.STRING, wp.OPTIONAL)]


class Scanner(HardwareDevice):
  
  OME_TABLE = 'Scanner'
  __fields__ = []


class Chip(Device):
  
  OME_TABLE = 'Chip'
  __fields__ = [('barcode', wp.STRING, wp.OPTIONAL)]


class AnnotatedChip(Chip):

  OME_TABLE = 'AnnotatedChip'
  __fields__ = [('annotationFile', OriginalFile, wp.REQUIRED)]


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

  def __cleanup__(self):
    if hasattr(self, 'target'):
      try:
        # destroy all the edges related to this action
        self.proxy.dt.destroy_edges(self)
      except AttributeError:
        # Not using the Neo4J driver
        pass
