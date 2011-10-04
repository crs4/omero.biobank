import wrapper as wp
from utils import assign_vid_and_timestamp, make_unique_key
import numpy as np

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
