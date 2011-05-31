import omero.model as om
import omero.rtypes as ort


import bl.vl.utils as vu
import bl.vl.utils.ome_utils as vou
import wrapper as wp

from action import Action, assing_vid_and_timestamp
from objects_collections import TiterPlate

class VesselContent(wp.OmeroWrapper):
  OME_TABLE = 'VesselContent'
  __enums__ = ["EMPTY", "BLOOD", "SERUM", "DNA"]


class VesselStatus(wp.OmeroWrapper):
  OME_TABLE = 'VesselStatus'
  __enums__ = ["UNUSED", "UNKNOWN", "UNUSABLE", "DESTROYED",
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
                ('action', Action, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    return assing_vid_and_timestamp(conf, time_stamp_field='activationDate')


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

  def __preprocess_conf__(self, conf):
    if not 'containerSlotLabelUK' in conf:
      clabel = conf['container'].label
      label   = conf['label']
      conf['containerSlotLabelUK'] = vou.make_unique_key(clabel, label)
    if not 'containerSlotIndexUK' in conf:
      clabel = conf['container'].label
      slot   = conf['slot']
      conf['containerSlotIndexUK'] = vou.make_unique_key(clabel, '%04d' % slot)
