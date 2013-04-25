# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Genotyping adapter
==================

This module adds genotyping support to the knowledge base.  The
central entity is the genetic marker, a mutation in the DNA that can
be used to study the relationship between a phenotypic trait and its
genetic cause.  We model a specific type of marker known as Single
Nucleotide Polymorphism (SNP).

SNPs are variations at a single position in a DNA sequence.  In most
cases, a SNP consists of only two variants, or alleles, customarily
denoted by the letters A and B.  Thus, for diploid organisms such as
humans, there are three possible genotype configurations at each SNP
site: AA, AB and BB.

a SNP is typically described by a mask in the <FLANK>[A/B]<FLANK>
format.  Allele order is defined by the order within the square
brackets.  The mask is expected to be in the TOP Illumina convention,
if the Illumina strand detection algorithm yields a result (see
:func:`~bl.vl.utils.snp.convert_to_top`).
"""

import itertools as it
from operator import itemgetter
from collections import Counter

import numpy as np

import bl.vl.utils as vlu
import bl.vl.utils.snp as vlu_snp
import bl.vl.utils.np_ext as np_ext

from utils import assign_vid, make_unique_key
import wrapper as wp


BATCH_SIZE = 5000
VID_SIZE = vlu.DEFAULT_VID_LEN

# mset tables
ALIGN_TABLE = 'align'
GDO_TABLE = 'gdo'
MSET_TABLE = 'mset'
MS_TABLES = frozenset([ALIGN_TABLE, GDO_TABLE, MSET_TABLE])


class Marker(object):
  """
  Wraps the contents of a marker definition and associate information.
  """
  def __init__(self, vid, index=None, position=(0,0), flip=None, **kwargs):
    self.id = vid
    self.index = index
    self.position = position
    self.flip = flip
    for k, v in kwargs.iteritems():
      setattr(self, k, v)

  # this is here to make app.importer.map_vid happy
  @classmethod
  def get_ome_table(klass):
    return klass.__name__


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
    Load marker alignment info wrt ``ref_genome``.
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


class GenotypingAdapter(object):

  SNP_FLANK_SIZE = vlu_snp.SNP_FLANK_SIZE
  SNP_MASK_SIZE = 2 * SNP_FLANK_SIZE + len("[A/B]")
  SNP_SET_COLS = [
    ('string', 'vid', 'Marker VID', VID_SIZE, None),
    ('string', 'label', 'Marker label', 48, None),
    ('string', 'mask', 'Illumina TOP mask in the <FLANK>[A/B]<FLANK> format',
     SNP_MASK_SIZE, None),
    ('long', 'index', "Marker index within this set", None),
    ('bool', 'allele_flip', 'True if the A/B convention is reversed', None),
    ('string', 'op_vid', 'Last operation that modified this row',
     VID_SIZE, None),
    ]
  SNP_ALIGNMENT_COLS = [
    ('string', 'marker_vid', 'Marker VID', VID_SIZE, None),
    ('string', 'ref_genome', 'Reference alignment genome', 16, None),
    ('long', 'chromosome', '1-22, 23(X), 24(Y), 25(XY), 26(MT)', None),
    ('long', 'pos', "Position on the chromosome wrt 5'", None),
    ('long', 'global_pos', "Overall position in the genome", None),
    ('bool', 'strand', 'True if aligned on reference strand', None),
    ('string', 'allele', 'Allele found at this position (A/B)', 1, None),
    ('long', 'copies', "Number of alignments for this marker", None),
    ('string', 'op_vid', 'Last operation that modified this row',
     VID_SIZE, None),
    ]
  @staticmethod
  def SNP_GDO_REPO_COLS(N):
    cols = [
      ('string', 'vid', 'gdo VID', VID_SIZE, None),
      ('string', 'op_vid', 'Last operation that modified this row',
       VID_SIZE, None),
      ('float_array', 'probs', 'np.zeros((2,N), dtype=np.float32)',
       2*N, None),
      ('float_array', 'confidence', 'np.zeros((N,), dtype=np.float32)',
       N, None),
      ]
    return cols

  def __init__(self, kb):
    self.kb = kb

  @classmethod
  def snp_markers_set_table_name(klass, table_name_root, set_vid):
    assert(table_name_root in MS_TABLES)
    return '%s-%s.h5' % (table_name_root, set_vid)

  @classmethod
  def snp_markers_set_table_name_parse(klass, table_name):
    tag, set_vid = table_name.rsplit('.', 1)[0].rsplit('-', 1)
    if tag not in MS_TABLES:
      raise ValueError('tag %s from %s is illegal' % (tag, table_name))
    return tag, set_vid

  def _create_snp_markers_set_table(self, table_name_root, cols_def, set_vid):
    table_name = self.snp_markers_set_table_name(table_name_root, set_vid)
    self.kb.create_table(table_name, cols_def)
    return set_vid

  def _delete_snp_markers_set_table(self, table_name_root, set_vid):
    table_name = self.snp_markers_set_table_name(table_name_root, set_vid)
    self.kb.delete_table(table_name)

  def _fill_snp_markers_set_table(self, table_name_root, set_vid, i_s,
                                  batch_size):
    table_name = self.snp_markers_set_table_name(table_name_root, set_vid)
    return self.kb.add_table_rows_from_stream(table_name, i_s, batch_size)

  def _read_snp_markers_set_table(self, table_name_root, set_vid, selector,
                                  batch_size):
    table_name = self.snp_markers_set_table_name(table_name_root, set_vid)
    return self.kb.get_table_rows(table_name, selector, batch_size=batch_size)

  def create_snp_markers_set_tables(self, set_vid, N):
    """
    Create all tables needed by a SNPMarkersSet.
    """
    snp_gdo_repo_cols = self.SNP_GDO_REPO_COLS(N)
    for table, cols in ((MSET_TABLE, self.SNP_SET_COLS),
                        (ALIGN_TABLE, self.SNP_ALIGNMENT_COLS),
                        (GDO_TABLE, snp_gdo_repo_cols)):
      self._create_snp_markers_set_table(table, cols, set_vid)

  def delete_snp_markers_set_tables(self, set_vid):
    """
    Delete all tables related to a SNPMarkersSet.
    """
    for table in MS_TABLES:
      self._delete_snp_markers_set_table(table, set_vid)

  def define_snp_markers_set(self, set_vid, stream, op_vid,
                             batch_size=BATCH_SIZE):
    """
    Fill in a SNPMarkersSet definition table.
    """
    N = [0]
    def mod_stream():
      for x in stream:
        assign_vid(x)
        x['op_vid'] = op_vid
        N[0] += 1
        yield x
    i_s = mod_stream()
    by_idx_s = iter(sorted(i_s, key=itemgetter('index')))  # uses memory
    self._fill_snp_markers_set_table(MSET_TABLE, set_vid, by_idx_s,
                                     batch_size=batch_size)
    return N[0]

  def read_snp_markers_set(self, set_vid, selector=None, batch_size=BATCH_SIZE):
    return self._read_snp_markers_set_table(MSET_TABLE, set_vid, selector,
                                            batch_size=batch_size)

  def add_snp_markers_set_alignments(self, mset, stream, action,
                                     batch_size=BATCH_SIZE):
    """
    Add alignment info to a SNPMarkersSet table.

    In the case of multiple hits, only the first copy encountered is
    added in the same order as it is found in the input stream;
    additional copies are temporarily stored and appended at the end.
    In addition, the global position field for all copies will be set
    to a unique negative value. Duplicate global positions (different
    SNPs align to the same position) will also be replaced by unique
    negative values. This is done to avoid ambiguities in marker set
    intersection.
    """
    tname = self.snp_markers_set_table_name(MSET_TABLE, mset.id)
    vids = [t[0] for t in
            self.kb.get_table_rows(tname, None, col_names=['vid'])]
    vids_set = frozenset(vids)
    global_pos = SNPMarkersSet.compute_global_position
    dummy_pos_generator = it.count(-1 - mset.omero_id * mset.MAX_LEN, -1)
    def add_vids(stream):
      multiple_hits = {}
      for x in stream:
        k = x['marker_vid']
        if k not in vids_set:
          continue
        x['op_vid'] = action.id
        if x['copies'] == 1:
          x['global_pos'] = global_pos((x['chromosome'], x['pos']))
        else:
          x['global_pos'] = dummy_pos_generator.next()
        if x['copies'] > 1:
          if k in multiple_hits:
            multiple_hits[k].append(x)
            continue
          else:
            multiple_hits[k] = []
        yield x
      for v in multiple_hits.itervalues():
        for x in v:
          yield x
    i_s = add_vids(stream)
    by_vid = {}
    gpos_count = Counter()
    for i in xrange(len(vids)):
      r = i_s.next()
      by_vid[r['marker_vid']] = r
      gpos_count[r['global_pos']] += 1
    records = []
    for v in vids:
      try:
        r = by_vid[v]
      except KeyError as e:
        raise ValueError("no alignment info for %s" % e.args[0])
      if gpos_count[r['global_pos']] > 1:
        r['global_pos'] = dummy_pos_generator.next()
      records.append(r)
    i_s = it.chain(iter(records), i_s)
    return self._fill_snp_markers_set_table(ALIGN_TABLE, mset.id, i_s,
                                            batch_size=batch_size)

  def read_snp_markers_set_alignments(self, set_vid, selector=None,
                                      batch_size=BATCH_SIZE):
    return self._read_snp_markers_set_table(ALIGN_TABLE, set_vid, selector,
                                            batch_size=batch_size)

  def add_gdo(self, set_vid, probs, confidence, op_vid):
    probs.shape = probs.size
    table_name = self.snp_markers_set_table_name(GDO_TABLE, set_vid)
    row = {'op_vid': op_vid, 'probs': probs, 'confidence': confidence}
    assign_vid(row)
    row_indices = self.kb.add_table_row(table_name, row)
    assert len(row_indices) == 1
    probs.shape = (2, probs.size/2)
    return row['vid'], row_indices[0]

  def _unwrap_gdo(self, row, indices):
    r = {'vid': row['vid'], 'op_vid': row['op_vid']}
    p = row['probs']
    p.shape = (2, p.size/2)
    r['probs'] = p[:, indices] if indices is not None else p
    c = row['confidence']
    r['confidence'] = c[indices] if indices is not None else c
    return r

  def get_gdo(self, set_vid, vid, row_index, indices=None):
    table_name = self.snp_markers_set_table_name(GDO_TABLE, set_vid)
    rows = self.kb.get_table_rows_by_indices(table_name, [row_index])
    assert len(rows) == 1
    assert rows[0]['vid'] == vid
    return self._unwrap_gdo(rows[0], indices)

  def get_gdo_iterator(self, set_vid, indices=None, batch_size=100):
    def iterator(stream):
      for d in stream:
        yield self._unwrap_gdo(d, indices)
    table_name = self.snp_markers_set_table_name(GDO_TABLE, set_vid)
    return iterator(
      self.kb.get_table_rows_iterator(table_name, batch_size=batch_size)
      )
