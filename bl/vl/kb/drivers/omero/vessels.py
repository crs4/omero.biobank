import wrapper as wp

from action import Action
from containers import TiterPlate

class VesselContent(wp.OmeroWrapper):
  OME_TABLE = 'VesselContent'
  __fields__ = []

class VesselStatus(wp.OmeroWrapper):
  OME_TABLE = 'VesselStatus'
  __fields__ = []

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
      conf['containerSlotLabelUK'] = vluo.make_unique_key(clabel, label)
    if not 'containerSlotIndexUK' in conf:
      clabel = conf['container'].label
      slot   = conf['slot']
      conf['containerSlotIndexUK'] = vluo.make_unique_key(clabel, slot)
