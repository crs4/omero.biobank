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

  MAX_LEN = 10**8
  MAX_GENOME_LEN = 10**10

  @staticmethod
  def compute_global_position(p):
    return p[0]*SNPMarkersSet.MAX_GENOME_LEN + p[1]

  @staticmethod
  def define_range_selector(mset, gc_range, closed_interval=True):
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
    if not mset.has_aligns():
      raise ValueError('aligns vector has not been loaded')
    beg, end = gc_range
    global_pos = mset.aligns['global_pos']
    idx = mset.markers['index']
    low_gpos = SNPMarkersSet.compute_global_position(beg)
    high_gpos = SNPMarkersSet.compute_global_position(end)
    sel = (low_gpos <= global_pos) &  (global_pos <= high_gpos)
    return idx[sel]

  @staticmethod
  def intersect(mset1, mset2):
    """
    Returns a pair of equal length numpy arrays where corresponding
    array elements are the indices of markers, respectively in mset1
    and mset2, that align to the same position on the same ref_genome.

    .. code-block:: python

      ref_genome = 'hg19'
      ms1.load_alignments(ref_genome)
      ms2.load_alignments(ref_genome)
      idx1, idx2 = kb.SNPMarkersSet.intersect(ms1, ms2)
      for i1, i2 in it.izip(idx1, idx2):
        assert ms1[i].position == ms2[i].position

    """
    if not (mset1.has_aligns() and mset2.has_aligns()):
      raise ValueError('both mset should be aligned')
    if mset1.ref_genome != mset2.ref_genome:
      raise ValueError('msets should be aligned to the same ref_genome')
    gpos1 = mset1.aligns['global_pos']
    gpos2 = mset2.aligns['global_pos']
    return np_ext.index_intersect(gpos1, gpos2)

  def __preprocess_conf__(self, conf):
    if not 'snpMarkersSetUK' in conf:
      conf['snpMarkersSetUK'] = make_unique_key(conf['maker'], conf['model'],
                                                conf['release'])
    return conf

  def __cleanup__(self):
    self.proxy.gadpt.delete_snp_markers_set_tables(self.id)

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
    kwargs = {
      'vid': mdef['vid'],
      'label': mdef['label'],
      'mask': mdef['mask'],
      'index': mdef['index'],
      'flip': mdef['allele_flip'],
      'position': (0, 0),
      }
    if self.has_aligns():
      mali = self.aligns[i]
      if mali['copies'] == 1:
        kwargs.update({'position' : (mali['chromosome'], mali['pos']),
                       'on_reference_strand' : mali['strand'],
                       'allele_on_reference' : mali['allele']})
    return Marker(**kwargs)

  def load_markers(self, batch_size=1000):
    """
    Read marker info from the marker set table and store it in the
    markers attribute.
    """
    data = self.proxy.gadpt.read_snp_markers_set(self.id, batch_size=batch_size)
    self.__set_markers(data)

  def load_alignments(self, ref_genome, batch_size=1000):
    """
    Load marker positions using known alignments on ref_genome.
    Markers that do not align will be forced to a global position
    equal to minus (marker_indx + SNPMarkersSet.MAX_LEN * omero_id of
    self). This is done to avoid ambiguities in mset intersection.
    """
    if not self.has_markers():
      raise ValueError('markers vector has not been reloaded')
    selector = "(ref_genome == '%s')" % ref_genome
    aligns = self.proxy.gadpt.read_snp_markers_set_alignments(
      self.id, selector=selector, batch_size=batch_size
      )
    assert len(aligns) >= len(self)
    if len(aligns) > len(self):
      aligns = aligns[:len(self)]
    no_align_positions =  - (
      self.markers['index'] + self.omero_id * self.MAX_LEN
      )
    aligns['global_pos'] = np.choose(
      aligns['copies'] == 1, [no_align_positions, aligns['global_pos']]
      )
    self.bare_setattr('aligns', aligns)
    self.bare_setattr('ref_genome', ref_genome)

  def get_markers_iterator(self, indices = None):
    if not self.has_markers():
      raise ValueError('markers vector has not been reloaded')
    for i in xrange(len(self)):
      yield self[i]

  def __update_constraints__(self):
    uk = make_unique_key(self.maker, self.model, self.release)
    setattr(self.ome_obj, 'snpMarkersSetUK',
            self.to_omero(self.__fields__['snpMarkersSetUK'][0], uk))
