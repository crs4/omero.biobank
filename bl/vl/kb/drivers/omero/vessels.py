# BEGIN_COPYRIGHT
# END_COPYRIGHT

import omero.model as om
import omero.rtypes as ort

import wrapper as wp
from action import Action
from utils import assign_vid_and_timestamp, make_unique_key, assign_vid
from objects_collections import VLCollection, Lane, TiterPlate

import re

class VesselContent(wp.OmeroWrapper):
  
  OME_TABLE = 'VesselContent'
  __enums__ = ["EMPTY", "BLOOD", "SERUM", "DNA", "RNA"]


class VesselStatus(wp.OmeroWrapper):
  
  OME_TABLE = 'VesselStatus'
  __enums__ = ["UNUSED", "UNKNOWN", "UNUSABLE", "DISCARDED",
               "CONTENTUSABLE", "CONTENTCORRUPTED"]


class Vessel(wp.OmeroWrapper):
  
  OME_TABLE = 'Vessel'
  __fields__ = [('vid',   wp.VID, wp.REQUIRED),
                ('activationDate', wp.TIMESTAMP, wp.REQUIRED),
                ('destructionDate', wp.TIMESTAMP, wp.OPTIONAL),
                ('currentVolume', wp.FLOAT, wp.REQUIRED),
                ('initialVolume', wp.FLOAT, wp.REQUIRED),
                ('content', VesselContent, wp.REQUIRED),
                ('status', VesselStatus, wp.REQUIRED),
                ('action', Action, wp.REQUIRED),
                ('lastUpdate', Action, wp.OPTIONAL)]

  def __preprocess_conf__(self, conf):
    if not 'activationDate'  in conf:
      return assign_vid_and_timestamp(conf, time_stamp_field='activationDate')
    else:
      return assign_vid(conf)


class Tube(Vessel):
  
  OME_TABLE = 'Tube'
  __fields__ = [('label', wp.STRING, wp.REQUIRED),
                ('barcode', wp.STRING, wp.OPTIONAL)]


class PlateWell(Vessel):
  
  OME_TABLE = 'PlateWell'
  __fields__ = [('label', wp.STRING, wp.REQUIRED),
                ('slot', wp.INT, wp.REQUIRED),
                ('container', TiterPlate, wp.REQUIRED),
                ('containerSlotLabelUK', wp.STRING, wp.REQUIRED),
                ('containerSlotIndexUK', wp.STRING, wp.REQUIRED)]

  def __slot_from_label(self, label, rows, cols):
    m = re.match('^([A-Z])(\d{1,2})$', label.upper())
    if not m:
      raise ValueError('label [%s] not in the form A1' % label)
    row, col = ord(m.groups()[0]) - ord('A'), int(m.groups()[1]) - 1
    if row >= rows or col >= cols:
      raise ValueError('label [%s] out of range', label)
    return row * cols + col

  def __label_from_slot(self, slot, rows, cols):
    row, col = (slot / cols),  (slot % cols)
    label = '%s%02d' % (chr(ord('A') + row), col + 1)
    return label

  def __preprocess_conf__(self, conf):
    super(PlateWell, self).__preprocess_conf__(conf)
    rows, cols = conf['container'].rows, conf['container'].columns
    if not 'slot' in conf:
      conf['slot'] = self.__slot_from_label(conf['label'], rows, cols)
    else:
      slot = conf['slot']
      rlabel = self.__label_from_slot(slot, rows, cols)
      if 'label' in conf:
        label = conf['label']
        if label.upper() != rlabel:
          raise ValueError('label [%s] inconsistent with slot [%d]'
                           % (label, slot))
        else:
          conf['label'] = rlabel
    if not 'containerSlotLabelUK' in conf:
      clabel = conf['container'].label
      label   = conf['label']
      conf['containerSlotLabelUK'] = make_unique_key(clabel, label)
    if not 'containerSlotIndexUK' in conf:
      clabel = conf['container'].label
      slot   = conf['slot']
      conf['containerSlotIndexUK'] = make_unique_key(clabel, '%04d' % slot)
    return conf

  def __update_constraints__(self):
    csl_uk = make_unique_key(self.container.label, self.label)
    setattr(self.ome_obj, 'containerSlotLabelUK',
            self.to_omero(self.__fields__['containerSlotLabelUK'][0], csl_uk))
    csi_uk = make_unique_key(self.container.label, '%04d' % self.slot)
    setattr(self.ome_obj, 'containerSlotIndexUK',
            self.to_omero(self.__fields__['containerSlotIndexUK'][0], csi_uk))


# These classes are not in objects_collections module in order to
# prevent a cyclic import issue
class VesselsCollection(VLCollection):
  
  OME_TABLE = 'VesselsCollection'
  __fields__ = []


class VesselsCollectionItem(wp.OmeroWrapper):

  OME_TABLE = 'VesselsCollectionItem'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('vessel', Vessel, wp.REQUIRED),
                ('vesselsCollection', VesselsCollection, wp.REQUIRED),
                ('vesselsCollectionItemUK', wp.STRING, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    if not 'vesselsCollectionItemUK' in conf:
      v_vid = conf['vessel'].id
      vc_vid = conf['vesselsCollection'].id
      conf['vesselsCollectionItemUK'] = make_unique_key(vc_vid, v_vid)
    return assign_vid(conf)

  def __update_contraints__(self):
    vci_uk = make_unique_key(self.vesselsCollection.id, self.vessel.id)
    setattr(self.ome_obj, 'vesselsCollectionItemUK',
            self.to_omero(self.__fields__['vesselsCollectionItemUK'][0], vci_uk))


class LaneSlot(wp.OmeroWrapper):

  OME_TABLE = 'LaneSlot'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('lane', Lane, wp.REQUIRED),
                ('tag', wp.STRING, wp.OPTIONAL),
                ('content', VesselContent, wp.REQUIRED),
                ('laneSlotUK', wp.STRING, wp.REQUIRED),
                ('action', Action, wp.REQUIRED),
                ('lastUpdate', Action, wp.OPTIONAL)]

  def __preprocess_conf__(self, conf):
    if not 'laneSlotUK' in conf:
      if 'tag' in conf:
        conf['laneSlotUK'] = make_unique_key(conf['tag'], conf['lane'].label)
      else:
        conf['laneSlotUK'] = make_unique_key(conf['lane'].label)
    return assign_vid(conf)

  def __update_constraints__(self):
    if self.tag:
      ls_uk = make_unique_key(self.tag, self.lane.label)
      setattr(self.ome_obj, 'laneSlotUK',
              self.to_omero(self.__field__['laneSlotUK'][0], ls_uk))
    else:
      ls_uk = make_unique_key(self.lane.label)
      setattr(self.ome_obj, 'laneSlotUK',
              self.to_omero(self.__field__['laneSlotUK'][0], ls_uk))

