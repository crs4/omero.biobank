import wrapper as wp

import numpy as np

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

  @classmethod
  def extract_range(mset, gc_range):
    """
    Returns a numpy array with the indices of the markers of mset that
    are contained in the provided gc_range. A gc_range is a two
    elements tuple, with each element a tuple (ref_genome, chromosome,
    position), where ref_genome is a str identifying the reference
    genome used, chromosome is an int in [1,26], and pos is a positive
    int. Both positional tuples should be for the same reference
    genome.  It is a responsibility of the caller to assure that mset
    has loaded markers definitions alligned on the provided reference genome.

    .. code-block:: python

      ref_genome = 'hg19'
      beg_chr = 10
      beg_pos = 190000
      end_chr = 10
      end_pos = 300000

      gc_begin=(ref_genome, begin_chrom, begin_pos)
      gc_end  =(ref_genome, end_chrom, end_pos)

      indices = kb.SNPMarkersSet.extract_range(mset0,
                                               gc_range=(gc_begin, gc_end))
      for i in indices:
        assert (beg_chr, beg_pos) <= mset.markers[i].pos < (end_chr, end_pos)


    """
    if gc_range[0][0] != gc_range[1][0]:
      msg = 'gc_range extremes should be on the same reference genome'
      raise ValueError(msg)

    if gc_range[0][0] != mset.ref_genome:
      msg = 'mset ref_genome is inconsistent with requested gc_range'
      raise ValueError(msg)

    beg = (gc_range[0][1], gc_range[0][2])
    end = (gc_range[1][1], gc_range[1][2])

    # FIXME brutal implementation
    indices = []
    for m in mset.markers():
      if beg <= m.pos < end:
        indices.append(i)
    return np.array(indices, dtype=np.int32)

  def __preprocess_conf__(self, conf):
    if not 'snpMarkersSetUK' in conf:
      conf['snpMarkersSetUK'] = make_unique_key(conf['maker'], conf['model'],
                                                conf['release'])
    return conf

  def has_markers(self):
    return hasattr(self, 'markers')

  def __set_markers(self, v):
    self.bare_setattr('markers', v)

  def __get_markers(self):
    return self.bare_getattr('markers')

  def __len__(self):
    if not self.has_markers():
      raise ValueError('markers vector has not been reloaded.')
    return len(self.markers)

  def __getitem__(self, x):
    if not self.has_markers():
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
    if not self.has_markers():
      raise ValueError('markers vector has not been reloaded.')

    self.proxy.update_snp_positions(self.markers, ref_genome)
    self.bare_setattr('ref_genome', ref_genome)

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
