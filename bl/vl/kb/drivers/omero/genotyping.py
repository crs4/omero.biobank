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

import bl.vl.utils as vlu
import bl.vl.utils.snp as vlu_snp
from utils import assign_vid


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

  def add_snp_markers_set_alignments(self, set_vid, stream, op_vid,
                                     batch_size=BATCH_SIZE):
    """
    Add alignment info to a SNPMarkersSet table.

    In the case of multiple hits, only the first copy encountered is
    added in the same order as it is found in the input stream;
    additional copies are temporarily stored and appended at the end.
    """
    tname = self.snp_markers_set_table_name(MSET_TABLE, set_vid)
    vids = [t[0] for t in
            self.kb.get_table_rows(tname, None, col_names=['vid'])]
    vids_set = frozenset(vids)
    def add_vids(stream):
      multiple_hits = {}
      for x in stream:
        k = x['marker_vid']
        if k not in vids_set:
          continue
        x['op_vid'] = op_vid
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
    for i in xrange(len(vids)):
      r = i_s.next()
      by_vid[r['marker_vid']] = r
    try:
      records = [by_vid[v] for v in vids]
    except KeyError as e:
      raise ValueError("no alignment info for %s" % e.args[0])
    i_s = it.chain(iter(records), i_s)
    return self._fill_snp_markers_set_table(ALIGN_TABLE, set_vid, i_s,
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
