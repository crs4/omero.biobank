import wrapper as wp

from action import Action

from utils import assign_vid_and_timestamp, make_unique_key

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


class DataObject(wp.OmeroWrapper):
  OME_TABLE = 'DataObject'
  __fields__ = [('sample', DataSample, wp.REQUIRED),
                 # following fields come from OriginalFile
                ('name',   wp.STRING,  wp.REQUIRED),
                ('path',   wp.STRING,  wp.REQUIRED),
                ('mimetype', wp.STRING, wp.REQUIRED),
                ('sha1',   wp.STRING, wp.REQUIRED),
                ('size',   wp.LONG,    wp.REQUIRED)]

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
  __enums__ = ["UNKNOWN",  "HUMANOMNI5-QUAD",
               "HUMANOMNI2.5S", "HUMANOMNI2.5-8", "HUMANOMNI1S",
               "HUMANOMNI1-QUAD", "HUMANOMNIEXPRESS", "HUMANCYTOSNP-12",
               "METABOCHIP", "IMMUNOCHIP"]

class IlluminaBeadChipAssay(MicroArrayMeasure):
  OME_TABLE = 'IlluminaBeadChipAssay'
  __fields__ = [('assayType', IlluminaBeadChipAssayType, wp.REQUIRED)]


class SNPMarkersSet(wp.OmeroWrapper):
  OME_TABLE = 'SNPMarkersSet'
  __fields__ = [('label', wp.STRING, wp.REQUIRED),
                ('maker', wp.STRING, wp.REQUIRED),
                ('model', wp.STRING, wp.REQUIRED),
                ('release', wp.STRING, wp.REQUIRED),
                ('markersSetVID', wp.VID, wp.REQUIRED),
                ('snpMarkersSetUK', wp.STRING, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    if not 'snpMarkersSetUK' in conf:
      conf['snpMarkersSetUK'] = make_unique_key(conf['maker'], conf['model'],
                                                conf['release'])
    return conf

class GenotypeDataSample(DataSample):
  OME_TABLE = 'GenotypeDataSample'
  __fields__ = [('snpMarkersSet', SNPMarkersSet, wp.REQUIRED)]






