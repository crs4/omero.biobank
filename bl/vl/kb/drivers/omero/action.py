# BEGIN_COPYRIGHT
# END_COPYRIGHT

import omero.model as om
import omero.rtypes as ort

from utils import assign_vid_and_timestamp, assign_vid, make_unique_key
import wrapper as wp
from snp_markers_set import SNPMarkersSet
from bl.vl.utils.graph import graph_driver


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
                ('description', wp.STRING, wp.OPTIONAL),
                ('labelUK', wp.STRING, wp.REQUIRED)]
  __do_not_serialize__ = ['labelUK']

  def __preprocess_conf__(self, conf):
    if not 'labelUK' in conf:
      conf['labelUK'] = make_unique_key(self.get_namespace(), conf['label'])
    return assign_vid_and_timestamp(conf, time_stamp_field='startDate')

  def __update_constraints__(self):
    label_uk = make_unique_key(self.get_namespace(), self.label)
    setattr(self.ome_obj, 'labelUK',
            self.to_omero(self.__fields__['labelUK'][0], label_uk))


class Device(wp.OmeroWrapper):
  
  OME_TABLE = 'Device'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('label', wp.STRING, wp.REQUIRED),
                ('maker', wp.STRING, wp.REQUIRED),
                ('model', wp.STRING, wp.REQUIRED),
                ('release', wp.STRING, wp.REQUIRED),
                ('labelUK', wp.STRING, wp.REQUIRED)]
  __do_not_serialize__ = ['labelUK']

  def __preprocess_conf__(self, conf):
    if not 'labelUK' in conf:
      conf['labelUK'] = make_unique_key(self.get_namespace(), conf['label'])
    return assign_vid(conf)

  def __update_constraints__(self):
    label_uk = make_unique_key(self.get_namespace(), self.label)
    setattr(self.ome_obj, 'labelUK',
            self.to_omero(self.__fields__['labelUK'][0], label_uk))


class SoftwareProgram(Device):
  
  OME_TABLE = 'SoftwareProgram'
  __fields__ = []


class GenotypingProgram(SoftwareProgram):
  
  OME_TABLE = 'GenotypingProgram'
  __fields__ = [('snpMarkersSet', SNPMarkersSet, wp.REQUIRED)]


class HardwareDevice(Device):
  
  OME_TABLE = 'HardwareDevice'
  __fields__ = [('barcode', wp.STRING, wp.OPTIONAL),
                ('physicalLocation', wp.STRING, wp.OPTIONAL),
                ('barcodeUK', wp.STRING, wp.OPTIONAL)]
  __do_not_serialize__ = ['barcodeUK']

  def __preprocess_conf__(self, conf):
    if not 'barcodeUK' in conf and conf.get('barcode'):
      conf['barcodeUK'] = make_unique_key(self.get_namespace(), conf['barcode'])
    return super(HardwareDevice, self).__preprocess_conf__(conf)

  def __update_constraints__(self):
    if self.barcode:
      b_uk = make_unique_key(self.get_namespace(), self.barcode)
      setattr(self.ome_obj, 'barcodeUK',
              self.to_omero(self.__field__['barcodeUK'][0], b_uk))
    super(HardwareDevice, self).__update_constraints__()


class Scanner(HardwareDevice):
  
  OME_TABLE = 'Scanner'
  __fields__ = []


class Chip(Device):
  
  OME_TABLE = 'Chip'
  __fields__ = [('barcode', wp.STRING, wp.OPTIONAL),
                ('barcodeUK', wp.STRING, wp.OPTIONAL)]
  __do_not_serialize__ = ['barcodeUK']

  def __preprocess_conf__(self, conf):
    if not 'barcodeUK' in conf and conf.get('barcode'):
      conf['barcodeUK'] = make_unique_key(self.get_namespace(), conf['barcode'])
    return super(Chip, self).__preprocess_conf__(conf)

  def __update_constraints__(self):
    if self.barcode:
      b_uk = make_unique_key(self.get_namespace(), self.barcode)
      setattr(self.ome_obj, 'barcodeUK',
              self.to_omero(self.__fields__['barcodeUK'][0], b_uk))
    super(Chip, self).__update_constraints__()


class AnnotatedChip(Chip):

  OME_TABLE = 'AnnotatedChip'
  __fields__ = [('annotationFile', OriginalFile, wp.REQUIRED)]


class ActionCategory(wp.OmeroWrapper):
  
  OME_TABLE = 'ActionCategory'
  __enums__ = ['IMPORT', 'CREATION', 'EXTRACTION', 'UPDATE',
               'ALIQUOTING', 'MEASUREMENT', 'PROCESSING',
               'OBJECT_CLONING']


class ActionSetup(wp.OmeroWrapper):
  
  OME_TABLE = 'ActionSetup'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('label', wp.STRING, wp.REQUIRED),
                ('conf', wp.STRING, wp.REQUIRED),
                ('labelUK', wp.STRING, wp.REQUIRED)]
  __do_not_serialize__ = ['labelUK']

  def __preprocess_conf__(self, conf):
    if not 'labelUK' in conf:
      conf['labelUK'] = make_unique_key(self.get_namespace(), conf['label'])
    return assign_vid(conf)

  def __update_constraints__(self):
    l_uk = make_unique_key(self.get_namespace(), self.label)
    setattr(self.ome_obj, 'labelUK',
            self.to_omero(self.__fields__['labelUK'][0], l_uk))


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
    # destroy all the edges related to this action
    if hasattr(self, 'target'):
      self.proxy.dt.destroy_edges(self)
