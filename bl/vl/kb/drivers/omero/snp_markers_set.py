# BEGIN_COPYRIGHT
# END_COPYRIGHT

import numpy as np

import bl.vl.utils.np_ext as np_ext
from utils import make_unique_key
import wrapper as wp

from genotyping import Marker


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

      ms.load_alignments(ref_genome)
      indices = kb.SNPMarkersSet.define_range_selector(
        ms,
        gc_range=(gc_begin, gc_end)
        )
      for i in indices:
        assert (beg_chr, beg_pos) <= ms.markers[i].position < (end_chr, end_pos)
    """
    beg, end = gc_range
    # FIXME inefficient implementation
    return np.array([i for i, m in enumerate(mset.markers)
                     if beg <= m.position < end], dtype=np.int32)

  def __preprocess_conf__(self, conf):
    if not 'snpMarkersSetUK' in conf:
      conf['snpMarkersSetUK'] = make_unique_key(conf['maker'], conf['model'],
                                                conf['release'])
    return conf

  @property
  def id(self):
    return self.markersSetVID

  def has_markers(self):
    return hasattr(self, 'markers')

  def has_aligns(self):
    return hasattr(self, 'aligns')

  def __set_markers(self, v):
    self.bare_setattr('markers', v)

  def __get_markers(self):
    return self.bare_getattr('markers')

  def __set_add_marker_info(self, v):
    self.bare_setattr('add_marker_info', v)

  def __get_add_marker_info(self, v):
    return self.bare_getattr('add_marker_info')

  def __set_aligns(self, v):
    self.bare_setattr('aligns', v)

  def __get_aligns(self):
    return self.bare_getattr('aligns')

  def __len__(self):
    if not self.has_markers():
      raise ValueError('markers vector has not been reloaded')
    return len(self.markers)

  def __nonzero__(self):
    return True

  def __getitem__(self, i):
    if not self.has_markers():
      raise ValueError('markers vector has not been reloaded')
    mdef = self.markers[i]
    pos = (0, 0)
    if self.has_aligns():
      mali = self.aligns[i]
      if mali['copies'] == 1:
        pos = (mali['chromosome'], mali['pos'])
    return Marker(mdef['marker_vid'], mdef['marker_indx'],
                  pos, flip=mdef['allele_flip'])

  def load_markers(self, batch_size=1000, additional_fields=None):
    """
    Read marker info from the marker set table and store it in the
    markers attribute.

    If additional_fields is provided, it must be a list of fields from
    the marker definition table; in this case, the additional info is
    stored in the add_marker_info attribute.
    """
    data = self.proxy.gadpt.read_snp_markers_set(self.id, batch_size=batch_size)
    data.sort(order='marker_indx')
    self.__set_markers(data)
    if additional_fields is not None:
      if "vid" not in additional_fields:
        additional_fields.append("vid")
      recs = self.proxy.get_snp_marker_definitions(col_names=additional_fields,
                                                   batch_size=batch_size)
      i1, i2 = np_ext.index_intersect(data['marker_vid'], recs['vid'])
      recs = recs[i2]
      # FIXME sort rows according to data's vid column
      self.__set_add_marker_info(recs)

  def load_alignments(self, ref_genome, batch_size=1000):
    """
    Load marker positions using known alignments on ref_genome.
    """
    if not self.has_markers():
      raise ValueError('markers vector has not been reloaded')
    aligns = self.proxy.gadpt.read_snp_markers_set_alignments(
      self.id, batch_size=batch_size
      )
    assert len(aligns) >= len(self)
    aligns = aligns[['chromosome', 'pos', 'global_pos', 'copies']]
    aligns = aligns[:len(self)]
    aligns['global_pos'] = np.choose(aligns['copies'] < 2,
                                     [0, aligns['global_pos']])
    self.bare_setattr('aligns', aligns)
    self.bare_setattr('ref_genome', ref_genome)

  def get_markers_iterator(self, indices = None):
    if not self.has_markers():
      raise ValueError('markers vector has not been reloaded')
    for i in xrange(len(self)):
      yield self[i]

  def __update_constraints__(self, base):
    uk = make_unique_key(self.maker, self.model, self.release)
    base.__setattr__(self, 'snpMarkersSetUK', uk)
