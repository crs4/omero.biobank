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
  __enums__ = ["ALS_ISELECT_272541_A",
        "CVDSNP55V1_A", "CARDIO_METABO_CHIP_11395247_A",
        "HUMAN1M","HUMAN1M_2",  "HUMAN1M_DUOV3_B",
        "HUMAN610_QUADV1_B", "HUMAN660W_QUAD_V1_A", "HUMANCNV370_QUADV3_C",
        "HUMANCNV370V1", "HUMANEXOME_12V1_A", "HUMANHAP250SV1_0",
        "HUMANHAP300V1_1", "HUMANHAP300V2_0", "HUMANHAP550V1_1",
        "HUMANHAP550V3_0", "HUMANHAP650YV1_0", "HUMANHAP650YV3_0",
        "HUMANOMNI1_QUAD_V1_0_B", "HUMANOMNI1_QUAD_V1_0_C",
        "HUMANOMNI2_5_4V1_B", "HUMANOMNI2_5_4V1_D", "HUMANOMNI2_5_4V1_H",
        "HUMANOMNI25EXOME_8V1_A", "HUMANOMNI5_4V1_B",
        "HUMANOMNIEXPRESSEXOME_8V1_A", "HUMANOMNIEXPRESS_12V1_C",
        "HUMANOMNIEXPRESS_12V1_MULTI_H", "IMMUNO_BEADCHIP_11419691_B",
        "LINKAGE_12", "UNKNOWN", "HUMAN1M_DUO", "HUMANOMNI5_QUAD",
        "HUMANOMNI2_5S", "HUMANOMNI2_5_8", "HUMANOMNI1S", "HUMANOMNI1_QUAD",
        "HUMANOMNIEXPRESS", "HUMANCYTOSNP_12", "METABOCHIP",
        "IMMUNOCHIP", "HUMANEXOME_12V1_B", "HUMANEXOME_12V1_1_A",
        "HUMANEXOME_12V1_2_A"]


class IlluminaArrayOfArraysType(wp.OmeroWrapper):
  OME_TABLE = "IlluminaArrayOfArraysType"
  __enums__ = ["BeadChip_12x1Q", "UNKNOWN", "BeadChip_12x8"]


class IlluminaArrayOfArraysClass(wp.OmeroWrapper):
  OME_TABLE = "IlluminaArrayOfArraysClass"
  __enums__ = ["Slide", "UNKNOWN"]


class IlluminaArrayOfArraysAssayType(wp.OmeroWrapper):
  OME_TABLE = "IlluminaArrayOfArraysAssayType"
  __enums__ = ["Infinium_HD", "UNKNOWN", "Infinium_NXT"]


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

  def _ibca_label_from_slot(self, slot, rows, cols):
    row, col = divmod(slot - 1, cols)
    return "R%02dC%02d" % (row+1, col+1)

  def _ibca_slot_from_label(self, label, rows, cols):
    m = re.match('^R(\d{2})C(\d{2})$', label)
    if m:
      row, col = map(int, m.groups())
      row -= 1
      if row >= rows or col > cols:
        raise ValueError('label [%s] out of range', label)
      return row * cols + col
    elif super(IlluminaBeadChipArray, self)._is_a_legal_label(label):
      return super(IlluminaBeadChipArray, self)._slot_from_label(label, rows, cols)
    else:
      raise ValueError('label [%s] not in a legal form' % label)

  def __preprocess_conf__(self, conf):
    # to pacify Vessel constructor
    conf['initialVolume'] = 0.0
    conf['currentVolume'] = 0.0
    if 'label' in conf:
      rows, cols = conf['container'].rows, conf['container'].columns
      new_slot = self._ibca_slot_from_label(conf['label'], rows, cols)
      if 'slot' in conf and conf['slot'] != new_slot:
        raise ValueError('inconsistent label %s and slot %s' %
                         (conf['label'], conf['slot']))
      conf['slot'] = new_slot
      conf['label'] = self._ibca_label_from_slot(conf['slot'], rows, cols)
      # conf.pop('label')
    return super(IlluminaBeadChipArray, self).__preprocess_conf__(conf)

  def __update_constraints__(self):
    csl_uk = make_unique_key(self.container.label, self.label)
    setattr(self.ome_obj, 'containerSlotLabelUK',
            self.to_omero(super(IlluminaBeadChipArray, self).__fields__['containerSlotLabelUK'][0],
                          csl_uk)
    )
    csi_uk = make_unique_key(self.container.label, '%04d' % self.slot)
    setattr(self.ome_obj, 'containerSlotIndexUK',
            self.to_omero(super(IlluminaBeadChipArray, self).__fields__['containerSlotIndexUK'][0],
                          csi_uk)
    )


class IlluminaBeadChipMeasure(MicroArrayMeasure):

  OME_TABLE = "IlluminaBeadChipMeasure"

  __fields__ = []


class IlluminaBeadChipMeasures(DataCollection):

  OME_TABLE = "IlluminaBeadChipMeasures"

  __fields__ = [('redChannel',   IlluminaBeadChipMeasure, wp.REQUIRED),
                ('greenChannel', IlluminaBeadChipMeasure, wp.REQUIRED)]




