# BEGIN_COPYRIGHT
# END_COPYRIGHT

from bl.vl.kb import mimetypes
import wrapper as wp
from action import Action, OriginalFile
from snp_markers_set import SNPMarkersSet
from utils import assign_vid_and_timestamp


class DataSampleStatus(wp.OmeroWrapper):

  OME_TABLE = 'DataSampleStatus'
  __enums__ = ["UNKNOWN", "DESTROYED", "CORRUPTED", "USABLE"]


class DataSample(wp.OmeroWrapper):

  OME_TABLE = 'DataSample'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('label', wp.STRING, wp.REQUIRED),
                ('creationDate', wp.TIMESTAMP, wp.REQUIRED),
                ('status', DataSampleStatus, wp.REQUIRED),
                ('action', Action, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    return assign_vid_and_timestamp(conf, time_stamp_field='creationDate')


class DataObject(OriginalFile):

  OME_TABLE = 'DataObject'
  __fields__ = [('sample', DataSample, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    conf['name'] = conf['sample'].vid
    return conf


class MicroArrayMeasure(DataSample):

  OME_TABLE = 'MicroArrayMeasure'
  __fields__ = []


class AffymetrixCelArrayType(wp.OmeroWrapper):

  OME_TABLE="AffymetrixCelArrayType"
  __enums__ = ["UNKNOWN", "GENOMEWIDESNP_6"]


class AffymetrixCel(MicroArrayMeasure):

  OME_TABLE = 'AffymetrixCel'
  __fields__ = [('arrayType', AffymetrixCelArrayType, wp.REQUIRED),
                ('celID',     wp.STRING,              wp.OPTIONAL)]


class IlluminaBeadChipAssayType(wp.OmeroWrapper):

  OME_TABLE="IlluminaBeadChipAssayType"
  __enums__ = ["UNKNOWN", "HUMAN1M_DUO",
               "HUMANOMNI5_QUAD",
               "HUMANOMNI2_5S", "HUMANOMNI2_5_8", "HUMANOMNI1S",
               "HUMANOMNI1_QUAD", "HUMANOMNIEXPRESS", "HUMANCYTOSNP_12",
               "METABOCHIP", "IMMUNOCHIP"]


class IlluminaBeadChipAssay(MicroArrayMeasure):

  OME_TABLE = 'IlluminaBeadChipAssay'
  __fields__ = [('assayType', IlluminaBeadChipAssayType, wp.REQUIRED)]


class GenotypeDataSample(DataSample):

  OME_TABLE = 'GenotypeDataSample'
  __fields__ = [('snpMarkersSet', SNPMarkersSet, wp.REQUIRED)]

  def resolve_to_data(self):
    dos = self.proxy.get_data_objects(self)
    if not dos:
      raise ValueError('no connected DataObject(s)')
    for do in dos:
      do.reload()
      if do.mimetype == mimetypes.GDO_TABLE:
        set_vid, vid, index = self.proxy.parse_gdo_path(do.path)
        mset = self.snpMarkersSet
        assert mset.id == set_vid
        mset.reload()
        res = self.proxy.get_gdo(mset, vid, index)
        return res['probs'], res['confidence']
    else:
      raise ValueError('DataObject is not a %s' % mimetypes.GDO_TABLE)


class SequencingDataSample(DataSample):
  
  OME_TABLE = 'SequencingDataSample'
  __fields__ = [('collectionIndex', wp.INT, wp.REQUIRED)]
