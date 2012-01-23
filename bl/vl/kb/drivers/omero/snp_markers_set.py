from itertools import izip
import numpy as np

from utils import make_unique_key
import wrapper as wp


class SNPMarkersSet(wp.OmeroWrapper):
  OME_TABLE = 'SNPMarkersSet'
  __fields__ = [('label', wp.STRING, wp.REQUIRED),
                ('maker', wp.STRING, wp.REQUIRED),
                ('model', wp.STRING, wp.REQUIRED),
                ('release', wp.STRING, wp.REQUIRED),
                ('markersSetVID', wp.VID, wp.REQUIRED),
                ('snpMarkersSetUK', wp.STRING, wp.REQUIRED)]

  @staticmethod
  def define_range_selector(mset, gc_range):
    """
    Returns a numpy array with the indices of the markers of mset that
    are contained in the provided gc_range. A gc_range is a two
    elements tuple, with each element a tuple (chromosome,
    position), where chromosome is an int in [1,26], and pos is a positive
    int. Both positional tuples should be for the same reference
    genome.  It is a responsibility of the caller to assure that mset
    has loaded markers definitions aligned on the same reference genome.

    .. code-block:: python

      ref_genome = 'hg19'
      beg_chr = 10
      beg_pos = 190000
      end_chr = 10
      end_pos = 300000

      gc_begin=(begin_chrom, begin_pos)
      gc_end  =(end_chrom, end_pos)

      mset0.load_alignments(ref_genome)
      indices = kb.SNPMarkersSet.define_range_selector(mset0,
                                                       gc_range=(gc_begin, gc_end))
      for i in indices:
        assert (beg_chr, beg_pos) <= mset.markers[i].position < (end_chr, end_pos)
    """
    beg, end = gc_range
    # FIXME brutal implementation
    indices = []
    for i, m in enumerate(mset.markers):
      if beg <= m.position < end:
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

  def __getitem__(self, i):
    if not self.has_markers():
      raise ValueError('markers vector has not been reloaded.')
    return self.markers[i]

  @property
  def id(self):
    return self.markersSetVID

  def load_markers(self, load_flip=False, batch_size=5000):
    """
    FIXME
    """
    self.reload()
    mdefs, msetc = self.proxy.get_snp_markers_set_content(self, batch_size)
    if load_flip:
      for (m, r) in izip(mdefs, msetc):
        m.flip = r['allele_flip']
    indices = msetc.argsort(order='marker_indx')
    mdefs = [mdefs[i] for i in indices]
    self.__set_markers(mdefs)

  def load_alignments(self, ref_genome):
    """
    Update markers position using known alignments on ref_genome.
    """
    if not self.has_markers():
      raise ValueError('markers vector has not been reloaded.')
    self.proxy.update_snp_positions(self.markers, ref_genome, ms_vid=self.id)
    self.bare_setattr('ref_genome', ref_genome)

  def __update_constraints__(self, base):
    uk = make_unique_key(self.maker, self.model, self.release)
    base.__setattr__(self, 'snpMarkersSetUK', uk)
