import omero.model as om
import omero.rtypes as ort

import wrapper as wp
from action import Action
from vessels import PlateWell
from objects_collections import TiterPlate, DataCollection
from data_samples import MicroArrayMeasure

from utils import assign_vid, make_unique_key

import re


class IlluminaBeadChipAssayType(wp.OmeroWrapper):

  OME_TABLE = "IlluminaBeadChipAssayType"
  __enums__ = ["ALS_iSelect_272541_A",
        "CVDSNP55v1_A", "Cardio_Metabo_Chip_11395247_A",
        "Human1M","Human1M_2",  "Human1M_Duov3_B",
        "Human610_Quadv1_B", "Human660W_Quad_v1_A", "HumanCNV370_Quadv3_C",
        "HumanCNV370v1", "HumanExome_12v1_A", "HumanHap250Sv1_0",
        "HumanHap300v1_1", "HumanHap300v2_0", "HumanHap550v1_1",
        "HumanHap550v3_0", "HumanHap650Yv1_0", "HumanHap650Yv3_0",
        "HumanOmni1_Quad_v1_0_B", "HumanOmni1_Quad_v1_0_C",
        "HumanOmni2_5_4v1_B", "HumanOmni2_5_4v1_D", "HumanOmni2_5_4v1_H",
        "HumanOmni25Exome_8v1_A", "HumanOmni5_4v1_B",
        "HumanOmniExpressExome_8v1_A", "HumanOmniExpress_12v1_C",
        "HumanOmniExpress_12v1_Multi_H", "Immuno_BeadChip_11419691_B",
        "Linkage_12", "UNKNOWN", "HUMAN1M_DUO", "HUMANOMNI5_QUAD",
        "HUMANOMNI2_5S", "HUMANOMNI2_5_8", "HUMANOMNI1S", "HUMANOMNI1_QUAD",
        "HUMANOMNIEXPRESS", "HUMANCYTOSNP_12", "METABOCHIP",
        "IMMUNOCHIP"]


class IlluminaArrayOfArraysType(wp.OmeroWrapper):
  OME_TABLE = "IlluminaArrayOfArraysType"
  __enums__ = ["BeadChip_12x1Q", "UNKNOWN"]


class IlluminaArrayOfArraysClass(wp.OmeroWrapper):
  OME_TABLE = "IlluminaArrayOfArraysClass"
  __enums__ = ["Slide", "UNKNOWN"]


class IlluminaArrayOfArraysAssayType(wp.OmeroWrapper):
  OME_TABLE = "IlluminaArrayOfArraysAssayType"
  __enums__ = ["Infinium_HD", "UNKNOWN"]


class IlluminaArrayOfArrays(TiterPlate):

  OME_TABLE = 'IlluminaArrayOfArrays'
  __fields__ = [('type', IlluminaArrayOfArraysType, wp.REQUIRED),
                ('arrayClass', IlluminaArrayOfArraysClass, wp.REQUIRED),
                ('assayType', IlluminaArrayOfArraysAssayType, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    return super(IlluminaArrayOfArrays, self).__preprocess_conf__(conf)


class IlluminaBeadChipArray(PlateWell):

  OME_TABLE = "IlluminaBeadChipArray"

  __fields__ = [('assayType', IlluminaBeadChipAssayType, wp.REQUIRED),
                ('container', IlluminaArrayOfArrays, wp.REQUIRED)]

  def _is_a_legal_label(self, label):
    return re.match('^R(\d{2})C(\d{2})$', label)

  def _ibca_slot_from_label(self, label, rows, cols):
    m = re.match('^R(\d{2})C(\d{2})$', label)
    if m:
      row, col = map(lambda x: int(x) - 1, m.groups())
      if row >= rows or col >= cols:
        raise ValueError('label [%s] out of range', label)
      return row * cols + col
    elif super(IlluminaBeadChipArray, self)._is_a_legal_label(label):
      return super(IlluminaBeadChipArray, self)\
                 ._ibca_slot_from_label(label, rows, cols)
    else:
      raise ValueError('label [%s] not in a legal form' % label)

  def __preprocess_conf__(self, conf):
    # to pacify Vessel constructor
    conf['initialVolume'] = 0.0
    conf['currentVolume'] = 0.0
    rows, cols = conf['container'].rows, conf['container'].columns
    if not 'slot' in conf:
      conf['slot'] = self._ibca_slot_from_label(conf['label'], rows, cols)
    else:
      if 'label' in conf:
        slot = self._ibca_slot_from_label(conf['label'], rows, cols)
        if slot != conf['slot']:
          raise ValueError('label [%s] inconsistent with slot [%d] conf: %r'
                           % (conf['label'], conf['slot'], conf))
    # normalize to a standard PlateWell label format
    conf['label'] = super(IlluminaBeadChipArray, self)\
                    ._label_from_slot(conf['slot'], rows, cols)
    return super(IlluminaBeadChipArray, self).__preprocess_conf__(conf)

  def __update_constraints__(self):
    super(IlluminaBeadChipArray, self).__update_constraints__(conf)


class IlluminaBeadChipMeasure(MicroArrayMeasure):

  OME_TABLE = "IlluminaBeadChipMeasure"

  __fields__ = []


class IlluminaBeadChipMeasures(DataCollection):

  OME_TABLE = "IlluminaBeadChipMeasures"

  __fields__ = [('redChannel',   IlluminaBeadChipMeasure, wp.REQUIRED),
                ('greenChannel', IlluminaBeadChipMeasure, wp.REQUIRED)]




