import omero.rtypes as ort

import bl.vl.sample.kb as kb

from bl.vl.sample.kb.drivers.omero.sample import DataSample

#------------------------------------------------------------
class AffymetrixCel(DataSample, kb.AffymetrixCel):

  OME_TABLE = "AffymetrixCel"

  LEGAL_ARRAY_TYPES = ['GenomeWideSNP_6']

  def __setup__(self, ome_obj, name, array_type, data_type, **kw):
    if name is None or array_type is None or data_type is None:
      raise ValueError('AffymetrixCel name, array_type and data_type cannot be None')
    if not array_type in self.LEGAL_ARRAY_TYPES:
      raise ValueError('%s not in %s' % (array_type, self.LEGAL_ARRAY_TYPES))
    ome_obj.arrayType = ort.rstring(array_type)
    super(AffymetrixCel, self).__setup__(ome_obj, name, data_type, **kw)

  def __init__(self, from_=None, name=None, array_type=None, data_type=None, **kw):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      ome_obj = ome_type()
      self.__setup__(ome_obj, name, array_type, data_type, **kw)
    super(AffymetrixCel, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.arrayType is None:
      raise kb.KBError("AffymetrixCel array_type can't be None")
    else:
      super(AffymetrixCel, self).__handle_validation_errors__()

#------------------------------------------------------------
class SNPMarkersSet(DataSample, kb.SNPMarkersSet):

  OME_TABLE = "SNPMarkersSet"

  def __setup__(self, ome_obj, name, maker, model, release, set_vid, data_type, **kw):
    if name is None or set_vid is None or data_type is None:
      raise ValueError("SNPMarkersSet name, maker, model, set_vid and data_type cannot be None")
    ome_obj.maker = ort.rstring(maker)
    ome_obj.model = ort.rstring(model)
    ome_obj.release = ort.rstring(release)
    super(SNPMarkersSet, self).__setup__(ome_obj, name, data_type, **kw)

  def __init__(self, from_=None, name=None, maker=None, model=None, release=None,
               data_type=None, **kw):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      ome_obj = ome_type()
      self.__setup__(ome_obj, name, maker, model, release, data_type, **kw)
    super(SNPMarkersSet, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.maker is None:
      raise kb.KBError("SNPMarkersSet maker can't be None")
    elif self.model is None:
      raise kb.KBError("SNPMarkersSet model can't be None")
    elif self.release is None:
      raise kb.KBError("SNPMarkersSet release can't be None")
    else:
      super(SNPMarkersSet, self).__handle_validation_errors__()

#------------------------------------------------------------
class GenotypeDataSample(DataSample, kb.GenotypeDataSample):

  OME_TABLE = "GenotypeDataSample"

  def __setup__(self, ome_obj, name, snp_markers_set, data_type, **kw):
    if name is None or snp_markers_set is None or data_type is None:
      raise ValueError('GenotypeDataSample name, snp_markers_set and data_type cannot be None')
    ome_obj.snpMarkersSet = snp_markers_set
    super(GenotypeDataSample, self).__setup__(ome_obj, name, data_type, **kw)

  def __init__(self, from_=None, name=None, snp_markers_set=None, data_type=None, **kw):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      ome_obj = ome_type()
      self.__setup__(ome_obj, name, snp_markers_set, data_type, **kw)
    super(GenotypeDataSample, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.snpMarkersSet is None:
      raise kb.KBError("GenotypeDataSample snpMarkersSet  can't be None")
    else:
      super(GenotypeDataSample, self).__handle_validation_errors__()
