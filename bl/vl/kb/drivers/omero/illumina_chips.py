import omero.model as om
import omero.rtypes as ort

import wrapper as wp
from action import Action
from vessels import PlateWell
from objects_collections import TiterPlate

from utils import assign_vid, make_unique_key

import re

class IlluminaAssayType(wp.OmeroWrapper):

  OME_TABLE = "IlluminaAssayType"
  __enums__ = ["ALS_iSelect_272541_A",
        "CVDSNP55v1_A", "Cardio_Metabo_Chip_11395247_A",
        "Human1M","Human1M_2",  "Human1M_Duov3_B",
        "Human610_Quadv1_B", "Human660W_Quad_v1_A", "HumanCNV370_Quadv3_C",
        "HumanCNV370v1", "HumanExome_12v1_A", "HumanHap250Sv1.0",
        "HumanHap300v1.1", "HumanHap300v2.0", "HumanHap550v1.1",
        "HumanHap550v3.0", "HumanHap650Yv1.0", "HumanHap650Yv3.0",
        "HumanOmni1_Quad_v1_0_B", "HumanOmni1_Quad_v1_0_C",
        "HumanOmni2.5_4v1_B", "HumanOmni2.5_4v1_D", "HumanOmni2.5_4v1_H",
        "HumanOmni25Exome_8v1_A", "HumanOmni5_4v1_B",
        "HumanOmniExpressExome_8v1_A", "HumanOmniExpress_12v1_C",
        "HumanOmniExpress_12v1_Multi_H", "Immuno_BeadChip_11419691_B",
        "Linkage_12", "UNKNOWN"]

class IlluminaArrayOfArrays(TiterPlate):

  OME_TABLE = 'IlluminaArrayOfArrays'
  __fields__ = []

  def __preprocess_conf__(self, conf):
    return super(IlluminaArrayOfArrays, self).__preprocess_conf__(conf)

class IlluminaBeadChipArray(PlateWell):

  OME_TABLE = "IlluminaBeadChipArray"

  __fields__ = [('assayType', IlluminaAssayType, wp.REQUIRED),
                ('container', IlluminaArrayOfArrays, wp.REQUIRED)]

  def _slot_from_label(self, label, rows, cols):
    m = re.match('^R(\d{2})C(\d{2})$', label)
    if not m:
      raise ValueError('label [%s] not in the form R%%02dC%%02d' % label)
    row, col = map(int, m.groups())
    row -= 1
    col -= 1
    if row >= rows or col >= cols:
      raise ValueError('label [%s] out of range', label)
    return row * cols + col

  def _label_from_slot(self, slot, rows, cols):
    row, col = (slot / cols),  (slot % cols)
    label = 'R%02dC%02d' % (row + 1, col + 1)
    return label

  def __preprocess_conf__(self, conf):
    # to pacify Vessel constructor
    conf['initialVolume'] = 0.0
    conf['currentVolume'] = 0.0
    rows, cols = conf['container'].rows, conf['container'].columns
    if not 'slot' in conf:
      conf['slot'] = self._slot_from_label(conf['label'], rows, cols)
    else:
      slot = conf['slot']
      rlabel = self._label_from_slot(slot, rows, cols)
      if 'label' in conf:
        label = conf['label']
        if label != rlabel:
          raise ValueError('label [%s] inconsistent with slot [%d]'
                           % (label, slot))
      else:
          conf['label'] = rlabel
    return super(IlluminaBeadChipArray, self).__preprocess_conf__(conf)


  def __update_constraints__(self):
    super(IlluminaBeadChipArray, self).__update_constraints__(conf)





