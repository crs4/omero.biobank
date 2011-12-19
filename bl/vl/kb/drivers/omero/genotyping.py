"""

.. todo::

   Explain why handling markers is a mess.

A SNP marker is defined by:

 * a SNP definition mask in the format <FLANK>[A/B]<FLANK>. Alleles
    order is defined by the order within the square brackets. The mask
    is is expected to be on the Illumina convention TOP strand, if the
    Illumina strand determination algorithm gets a definite result.

 * a string, '''source''', declaring the origin of the marker
   definition. E.g., 'Affymetrix', 'Illumina', 'ABI'

 * a string, '''context''', declaring the context of the
   definition. E.g., 'TaqMan-SNP_Genotyping_Assays'

 * a string, '''release''', declaring the specific release within the
   context. E.g., '12-Nov-2010'

 * a string, '''label''', declaring how this marker is known within
   the triple source, context, release above.

SNP markers could have a dbSNP assigned rs_label. Unfortunately, this
is a property of the marker that is conditioned on a specific dbSNP
release and a specific reference genome. In fact, the procedure we follow,

.. todo::

  add link to rs label identification procedure


The association of markers to their rs label is based on the comparison of
the aligment position against a given reference genome of the marker
mask with what is obtained by aligning all the SNP markers of dbSNP
against the same reference genome.  To keep track of this, each marker
record has the following additional fields:

  * a string, '''rs_label''', declaring the label of the marker in
    dbSNP that correspond to this specific marker. When no
    correspondence is known, it is set to the same value of the
    '''label''' field.

  * a long, '''dbSNP_build''', declaring the dbSNP markers build used
    in the identification procedure.

  * a string, '''ref_rs_genome''', declaring the reference genome used
    to find the rs corrispondence.

"""

import numpy as np
import bl.vl.utils as vlu
import bl.vl.utils.snp as vlu_snp


BATCH_SIZE = 5000
VID_SIZE = vlu.DEFAULT_VID_LEN


class Marker(object):
  """
  This is a wrapper used to export the contents of
  a snp marker definition and associate information as a python object.

  .. todo::

    define args.

  """

  def __init__(self, vid, label=None, rs_label=None,
               source=None, context=None, release=None,
               dbSNP_build=None, ref_rs_genome=None,
               mask=None, position=(0,0), flip=None):
    self.id = vid
    self.label = label
    self.rs_label = rs_label
    self.source = source
    self.context = context
    self.dbSNP_build = int(dbSNP_build) if dbSNP_build else None
    self.ref_rs_genome = ref_rs_genome
    self.mask = mask
    self.position = position
    self.flip = flip

  @classmethod
  def get_ome_table(klass):
    "FIXME this is to make app.importer.map_vid happy"
    return klass.__name__


class GenotypingAdapter(object):
  """
  FIXME
  """
  SNP_MARKER_DEFINITIONS_TABLE = 'snp_marker_definitions.h5'
  SNP_ALIGNMENT_TABLE  = 'snp_alignment.h5'
  SNP_SET_DEF_TABLE    = 'snp_set_def.h5'
  SNP_SET_TABLE        = 'snp_set.h5'
  SNP_FLANK_SIZE       = vlu_snp.SNP_FLANK_SIZE
  SNP_MASK_SIZE        = 2 * SNP_FLANK_SIZE + len("[A/B]")

  SNP_MARKER_DEFINITIONS_COLS = \
  [('string', 'vid',    'This marker VID', VID_SIZE, None),
   ('string', 'source', 'Origin of this marker definition.', 16, None),
   ('string', 'context', 'Context of definition.', 16, None),
   ('string', 'release', 'Release within the context.', 16, None),
   ('string', 'label', 'Label of marker in the definition context.', 48, None),
   ('string', 'rs_label', 'dbSNP_id if available', 32, None),  # too small!
   ('long',   'dbSNP_build', 'dbSNP build version.', None),
   ('string', 'ref_rs_genome', 'Reference rs alignment genome.', 16, None),
   ('string', 'mask',
    """SNP definition mask in the format <FLANK>[A/B]<FLANK>. It expected to be
    on the Illumina convention TOP strand.""", SNP_MASK_SIZE, None),
   ('string', 'op_vid', 'Last operation that modified this row',
    VID_SIZE, None)]

  SNP_ALIGNMENT_COLS = \
  [('string', 'marker_vid', 'VID of the aligned marker.', VID_SIZE, None),
   ('string', 'ref_genome', 'Reference alignment genome.', 16, None),
   ('long', 'chromosome',
    'Chromosome where this alignment was found. 1-22, 23(X) 24(Y) 25(XY) 26(MT)',
    None),
   ('long', 'pos', "Position on the chromosome. Starting from 5'", None),
   ('long', 'global_pos', "Global position in the genome. (chr*10**10 + pos)", None),
   ('bool', 'strand', 'Aligned on reference strand', None),
   # I know that this is in principle a bool, but what happens if we have more than two alleles?
   ('string', 'allele', 'Allele found at this position (A/B)', 1, None),
   ('long', 'copies', "Number of copies found for this marker within this alignment op.", None),
   ('string', 'op_vid', 'Last operation that modified this row', VID_SIZE, None)]

  SNP_SET_COLS = \
  [('string', 'vid', 'Set VID', VID_SIZE, None),
   ('string', 'marker_vid', 'Marker VID', VID_SIZE, None),
   ('long', 'marker_indx',
    "Ordered position of this marker within the set", None),
   ('bool', 'allele_flip',
    'Is this technology flipping our A/B allele convention?', None),
   ('string', 'op_vid',
    'Last operation that modified this row', VID_SIZE, None)]

  SNP_SET_DEF_COLS = \
  [('string', 'vid', 'Set VID', VID_SIZE, None),
   ('string', 'maker', 'Maker identifier.', 32, None),
   ('string', 'model', 'Model identifier.', 32, None),
   ('string', 'release', 'Release identifier.', 32, None),
   ('string', 'op_vid', 'Last operation that modified this row',
    VID_SIZE, None)]

  @classmethod
  def SNP_GDO_REPO_COLS(klass, N):
    cols = [('string', 'vid', 'gdo VID', VID_SIZE, None),
            ('string', 'op_vid', 'Last operation that modified this row',
             VID_SIZE, None),
            ('string', 'probs', 'np.zeros((2,N), dtype=np.float32).tostring()',
             2*N*4, None),
            ('string', 'confidence', 'np.zeros((N,), dtype=np.float32).tostring()',
             N*4, None)]
    return cols

  def __init__(self, kb):
    self.kb = kb

  #-- markers definitions
  def create_snp_marker_definitions_table(self):
    self.kb.create_table(self.SNP_MARKER_DEFINITIONS_TABLE,
                         self.SNP_MARKER_DEFINITIONS_COLS)

  def add_snp_marker_definitions(self, stream, op_vid, batch_size=BATCH_SIZE):
    vid_correspondence = []
    def add_vid_filter_and_op_vid(stream, op_vid):
      for x in stream:
        x['vid'] = vlu.make_vid()
        x['op_vid'] = op_vid
        vid_correspondence.append((x['label'], x['vid']))
        yield x
    i_s = add_vid_filter_and_op_vid(stream, op_vid)
    self.kb.add_table_rows_from_stream(self.SNP_MARKER_DEFINITIONS_TABLE,
                                       i_s, batch_size=batch_size)
    return vid_correspondence

  def get_snp_marker_definitions(self, selector=None, col_names=None,
                                 batch_size=BATCH_SIZE):
    return self.kb.get_table_rows(self.SNP_MARKER_DEFINITIONS_TABLE,
                                  selector, col_names, batch_size=batch_size)

  def marker_maker(self, r, names=None):
    if names is None:
      names = ['vid']
    elif 'vid' not in names:
      raise ValueError("need at least the VID field to create marker")
    args = dict(zip(names, r))
    return Marker(**args)

  def get_snp_markers_by_source(self, source, context=None, release=None,
                                col_names=None):
    selector = '(source=="%s")' % source
    if context:
      selector += '&(context=="%s")' % context
    # FIXME: unclear if release without a context is meaningful.
    if release:
      selector += '&(release=="%s")' % release
    recs = self.get_snp_marker_definitions(selector=selector)
    return [self.marker_maker(r, col_names) for r in recs]


  def get_snp_markers(self, labels=None, rs_labels=None, vids=None,
                      col_names=None,
                      batch_size=BATCH_SIZE):
    """
    Return a list of marker objects corresponding to the given list
    (labels, rs_labels or vids). Return an empty list if at least one
    of the items in the list does not correspond to any marker.

    .. todo::

       add documentation on col_names
    
    """
    count = (labels is None) + (rs_labels is None) + (vids is None)
    if count == 3:
      raise ValueError('labels, rs_labels and vids cannot be all None')
    if count == 1:
      raise ValueError('only one of labels, rs_labels and vids should be assigned')
    if labels:
      field_name = 'label'
      requested = labels
    elif rs_labels:
      field_name = 'rs_label'
      requested = rs_labels
    else:
      field_name = 'vid'
      requested = vids
    recs = self.get_snp_marker_definitions(col_names=[field_name],
                                           batch_size=max(batch_size,
                                                          len(requested)))
    by_field = dict(((l[0], i) for i, l in enumerate(recs)))
    row_indices = []
    for x in requested:
      try:
        row_indices.append(by_field[x])
      except KeyError:
        return []
    res = self.kb.get_table_slice(self.SNP_MARKER_DEFINITIONS_TABLE,
                                  row_indices, col_names, batch_size)
    return [self.marker_maker(r, col_names) for r in res]

  #-- marker sets
  def create_snp_markers_set_table(self):
    self.kb.create_table(self.SNP_SET_DEF_TABLE, self.SNP_SET_DEF_COLS)

  def create_snp_set_table(self):
    self.kb.create_table(self.SNP_SET_TABLE, self.SNP_SET_COLS)

  def snp_markers_set_exists(self, maker, model, release='1.0'):
    selector = ("(maker=='%s') & (model=='%s') & (release=='%s')" %
                (maker, model, release))
    if len(self.get_snp_markers_sets(selector)) > 0 :
      return True
    return False

  def add_snp_markers_set(self, maker, model, release, op_vid):
    if self.snp_markers_set_exists(maker, model, release):
      raise ValueError('SNP_MARKERS_SET(%s, %s, %s) is already in kb.' %
                       (maker, model, release))
    set_vid = vlu.make_vid()
    row = {'vid':set_vid, 'maker': maker, 'model' : model, 'release' : release,
           'op_vid':op_vid}
    self.kb.add_table_row(self.SNP_SET_DEF_TABLE, row)
    return set_vid

  def get_snp_markers_sets(self, selector=None, batch_size=BATCH_SIZE):
    return self.kb.get_table_rows(self.SNP_SET_DEF_TABLE, selector,
                                  batch_size=batch_size)

  def fill_snp_markers_set(self, set_vid, stream, op_vid,
                           batch_size=BATCH_SIZE):
    def add_op_vid(stream, N):
      for x in stream:
        x['vid'], x['op_vid'] = set_vid, op_vid
        N[0] += 1
        yield x
    N = [0]
    i_s = add_op_vid(stream, N)
    self.kb.add_table_rows_from_stream(self.SNP_SET_TABLE, i_s,
                                       batch_size=batch_size)
    return N[0]

  def get_snp_markers_set(self, selector=None, batch_size=BATCH_SIZE):
    return self.kb.get_table_rows(self.SNP_SET_TABLE, selector,
                                  batch_size=batch_size)

  #-- alignment
  def create_snp_alignment_table(self):
    self.kb.create_table(self.SNP_ALIGNMENT_TABLE, self.SNP_ALIGNMENT_COLS)

  def add_snp_alignments(self, stream, op_vid, batch_size=BATCH_SIZE):
    def add_op_vid(stream):
      for x in stream:
        x['op_vid'] = op_vid
        yield x
    i_s = add_op_vid(stream)
    return self.kb.add_table_rows_from_stream(self.SNP_ALIGNMENT_TABLE,
                                              i_s, batch_size)

  def get_snp_alignments(self, selector=None, col_names=None, batch_size=BATCH_SIZE):
    return self.kb.get_table_rows(self.SNP_ALIGNMENT_TABLE, selector,
                                  col_names=col_names, batch_size=batch_size)

  def get_snp_alignment_positions(self, ref_genome, marker_vids,
                                  batch_size=BATCH_SIZE):
    selector = '(ref_genome == "%s")' % ref_genome
    res = self.get_snp_alignments(selector, col_names=['marker_vid'],
                                  batch_size=batch_size)
    if len(res) == 0:
      return []
    by_vid = dict(((l[0], i) for i, l in enumerate(res)))
    row_indices = [by_vid[x] for x in marker_vids]
    return self.kb.get_table_slice(self.SNP_ALIGNMENT_TABLE,
                                   row_indices, ['chromosome', 'pos'],
                                   batch_size=batch_size)

  #-- gdo
  def _gdo_table_name(self, set_vid):
    return '%s.h5' % set_vid

  def create_gdo_repository(self, set_vid, N):
    table_name = self._gdo_table_name(set_vid)
    self.kb.create_table(table_name, self.SNP_GDO_REPO_COLS(N))
    return set_vid

  def add_gdo(self, set_vid, probs, confidence, op_vid):
    pstr = probs.tostring()
    cstr = confidence.tostring()
    assert len(pstr) == 2*len(cstr)
    #--
    table_name = self._gdo_table_name(set_vid)
    row = {'vid' : vlu.make_vid(), 'op_vid' : op_vid,
           'probs' :  pstr, 'confidence' : cstr}
    self.kb.add_table_row(table_name, row)
    # return (vid, mimetype, path)
    return (table_name, row['vid'])

  def __normalize_size(self, string, size):
    return string + chr(0) * (size - len(string))

  def __unwrap_gdo(self, set_id, row, indices=None):
    r = {'vid' :  row['vid'], 'op_vid' : row['op_vid'], 'set_id' : set_id}
    #--
    p = np.fromstring(self.__normalize_size(row['probs'],
                                            row.dtype['probs'].itemsize),
                      dtype=np.float32)
    p.shape = (2, p.shape[0]/2)
    r['probs'] = p[:, indices] if indices is not None else p
    #--
    c = np.fromstring(self.__normalize_size(row['confidence'],
                                            row.dtype['confidence'].itemsize),
                      dtype=np.float32)
    r['confidence'] = c[indices] if indices is not None else c
    #--
    return r

  def get_gdo(self, set_vid, vid, indices=None):
    table_name = self._gdo_table_name(set_vid)
    rows = self.kb.get_table_rows(table_name, selector='(vid == "%s")' % vid)
    assert len(rows) == 1
    return self.__unwrap_gdo(set_vid, rows[0], indices)

  def get_gdo_iterator(self, set_vid, indices=None, batch_size=100):
    def iterator(stream):
      for d in stream:
        yield self.__unwrap_gdo(set_vid, d, indices)
    table_name = self._gdo_table_name(set_vid)
    return iterator(self.kb.get_table_rows_iterator(table_name,
                                                    batch_size=batch_size))
