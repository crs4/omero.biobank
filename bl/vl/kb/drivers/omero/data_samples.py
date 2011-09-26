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
  __enums__ = ["UNKNOWN", "HUMAN1M_DUO",
               "HUMANOMNI5_QUAD",
               "HUMANOMNI2_5S", "HUMANOMNI2_5_8", "HUMANOMNI1S",
               "HUMANOMNI1_QUAD", "HUMANOMNIEXPRESS", "HUMANCYTOSNP_12",
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

  def __set_markers(self, v):
    self.bare_setattr('markers', v)

  def __get_markers(self):
    return self.bare_getattr('markers')

  def __len__(self):
    if not hasattr(self, 'markers'):
      raise ValueError('markers vector has not been reloaded.')
    return len(self.markers)

  def __getitem__(self, x):
    if not hasattr(self, 'markers'):
      raise ValueError('markers vector has not been reloaded.')
    return self.markers[x]

  @property
  def id(self):
    return self.markersSetVID

  def load_markers(self):
    self.reload()
    mdefs, msetc = self.proxy.get_snp_markers_set_content(self)
    self.__set_markers(mdefs)

  def load_alignments(self, ref_genome):
    """
    Update markers position using known alignments on ref_genome.
    """
    if not hasattr(self, 'markers'):
      raise ValueError('markers vector has not been reloaded.')

    self.proxy.update_snp_positions(self.markers, ref_genome)


class GenotypeDataSample(DataSample):
  OME_TABLE = 'GenotypeDataSample'
  __fields__ = [('snpMarkersSet', SNPMarkersSet, wp.REQUIRED)]

  def resolve_to_data(self):
    dos = self.proxy.get_data_objects(self)
    if not dos:
      raise ValueError('no connected DataObject(s)')
    do = dos[0]
    do.reload()
    if do.mimetype != 'x-bl/gdo-table':
      raise ValueError('DataObject is not a x-bl/gdo-table')
    jnk, vid = do.path.split('=')
    mset = self.snpMarkersSet
    mset.reload()
    res = self.proxy.get_gdo(mset.id, vid)
    return res['probs'], res['confidence']
