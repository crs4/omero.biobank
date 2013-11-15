"""
Genomics adapter
================

This module adds genetics and genomic support to the knowledge base.
The central entity is the genetic marker, a mutation in the DNA that
can be used to study the relationship between a phenotypic trait and
its genetic cause.  We use VCF like conventions to map multiple types
of markers, mainly Single Nucleotide Polymorphism (SNP).

We describe markers by a mask in the <FLANK>[VARIANT]<FLANK>
format.  Allele order is defined by the order within the square
brackets.  The mask is expected to be in the TOP Illumina convention,
if the Illumina strand detection algorithm yields a result (see
:func:`~bl.vl.utils.snp.convert_to_top`).

VARIANT is in the form A/B for a simple SNP, A/B,C for a multiple
allele marker, A,B,C,.. could be arbitrary bases sequences, with the
special case A = - to indicate a deletion. It is possible that, for a
specific marker, a SNP say, the order in which alleles are saved in
datafiles produced by a specific technology is a permutation of the
one described by VARIANT. In that case, VARIANT should have the form
A{1]/B{2},C{0} with the number within curly bracket that map the data
alleles to the variant indicated in VARIANT. Thus, the string above
means that the first, second and third allele indicated for that
specific marker in a dataset coming from this technology correspond,
respectively, to the third, first and second in VARIANT.


FIMXE
SNPs are variations at a single position in a DNA sequence.  In most
cases, a SNP consists of only two variants, or alleles, customarily
denoted by the letters A and B.  Thus, for diploid organisms such as
humans, there are three possible genotype configurations at each SNP
site: AA, AB and BB.

"""

import bl.vl.utils as vlu
from bl.vl.kb import mimetypes
import variant_call_support
import wrapper as wp
from proxy_core import convert_from_numpy
from utils import assign_vid, make_unique_key

import numpy as np
import hashlib

BATCH_SIZE = 5000
VID_SIZE = vlu.DEFAULT_VID_LEN

MARKER_LABEL_SIZE = 128
MARKER_MASK_SIZE = 1024 # this is probably way too big...
MSET_TABLE_NAME = 'mset'
MSET_TABLE_COLS_DTYPE  = [('label', '|S%d' % MARKER_LABEL_SIZE),
                          ('index', 'i8'),
                          ('mask',  '|S%d' % MARKER_MASK_SIZE),
                          ('permutation', '?'),
                          ('op_vid', '|S%d' % VID_SIZE)]
MSET_TABLE_COLS  = [
    ('string', 'label', 'Marker label', MARKER_LABEL_SIZE, None),
    ('long',   'index', 'Marker index within this set', None),
    ('string', 'mask', 'Illumina TOP mask in the <FLANK>[VAR]<FLANK> format',
     MARKER_MASK_SIZE, None),
    ('bool', 'permutation', 'Is VAR in data a permutation of VAR in mask?', 
     None),
    # FIXME op_vid is kept until we change SNPMarkersSet() to
    # MarkersArray(DataSample)
    ('string', 'op_vid', 'Last operation that modified this row',
     VID_SIZE, None),
    ]

GDO_TABLE_NAME = 'gdo'
def GDO_TABLE_COLS(N):
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

MA_TABLES = frozenset([MSET_TABLE_NAME, GDO_TABLE_NAME])

class GenomicsAdapter(object):
    """
    FIXME

  
    """

    def __init__(self, kb):
        self.kb = kb

    def create_markers_array(self, label, maker, model, release, rows, 
                             action):
        """
        Create a new (SNP)MarkersSet object and associate to it all
        the markers information contained in rows.  Rows could be
        either a numpy record array consistent with
        self.MSET_TABLE_COLS_DTYPE or a stream of dict records each of
        which should be consistent with self.MSET_TABLE_COLS. The
        order of markers array records exactly the one given in
        stream.

        FIXME: confusedly enough, this currently returns a SNPMarkersSet
        """
        avid = self.kb.resolve_action_id(action)
        conf = {'label': label, 'maker': maker, 'model': model, 
                'markersSetVID': vlu.make_vid(),
                'release': release}
        marray = self.kb.factory.create(self.kb.SNPMarkersSet, conf).save()
        self._create_markers_array_table(MSET_TABLE_NAME, MSET_TABLE_COLS, 
                                         marray.id)
        N = len(self._fill_markers_array_table(MSET_TABLE_NAME, marray.id,
                                               rows, avid,
                                               batch_size=BATCH_SIZE))
        #FIXME we are actually considering only SNP gdo.
        self._create_markers_array_table(GDO_TABLE_NAME, GDO_TABLE_COLS(N),
                                         marray.id)
        return marray

    def get_markers_array(self, label=None,
                          maker=None, model=None, release=None, vid=None):
        if label is not None:
            query = "select ms from SNPMarkersSet ms where ms.label = :label"
            pars = self.kb.ome_query_params({'label': wp.ome_wrap(label)})
        elif vid is not None:            
            query = """select ms from SNPMarkersSet ms 
                              where ms.markersSetVID = :vid"""
            pars = self.kb.ome_query_params({'vid': wp.ome_wrap(vid)})
        else:
            if not (maker and model and release):
                raise ValueError('maker, model, release should be all provided')
            query = """select ms from SNPMarkersSet ms
                       where ms.maker = :maker and ms.model = :model 
                                               and ms.release = :release"""
            pars = self.kb.ome_query_params({
                'maker': wp.ome_wrap(maker),
                'model': wp.ome_wrap(model),
                'release': wp.ome_wrap(release),
                })
        result = self.kb.ome_operation("getQueryService", "findByQuery",
                                       query, pars)
        return None if result is None else self.kb.factory.wrap(result)

    def get_markers_array_rows(self, marray, indices=None, 
                               batch_size=BATCH_SIZE):
        "FIXME"
        table_name = self._markers_array_table_name(MSET_TABLE_NAME, marray.id)
        return self.kb.get_table_rows_by_indices(table_name, indices,
                                                 col_names=None,
                                                 batch_size=batch_size)

    def get_number_of_markers(self, marray):
        "get the number of markers listed by marray"
        table_name = self._markers_array_table_name(MSET_TABLE_NAME, marray.id)
        return self.kb.get_number_of_rows(table_name)
        
    def delete_markers_array_tables(self, marray_id):
        for table in MA_TABLES:
            table_name = self._markers_array_table_name(table, marray_id)
            self.kb.delete_table(table_name)

    def make_gdo_path(self, marray, vid, index):
        table_name = self._markers_array_table_name(GDO_TABLE_NAME, marray.id)
        return 'table:%s/vid=%s/row_index=%d' % (table_name, vid, index)

    def parse_gdo_path(self, path):
        head, vid, index = path.split('/')
        head = head[len('table:'):]
        vid = vid[len('vid='):]
        tag, set_vid = self._markers_array_table_name_parse(head)
        index = int(index[len('row_index='):])
        return set_vid, vid, index

    def add_gdo(self, set_vid, probs, confidence, op_vid):
        probs.shape = probs.size
        table_name = self._markers_array_table_name(GDO_TABLE_NAME, set_vid)
        row = {'op_vid': op_vid, 'probs': probs, 'confidence': confidence}
        assign_vid(row)
        row_indices = self.kb.add_table_row(table_name, row)
        assert len(row_indices) == 1
        probs.shape = (2, probs.size/2)
        return row['vid'], row_indices[0]

    def add_gdo_data_object(self, action, sample, probs, confs):
        """
        Syntactic sugar to simplify adding genotype data objects.

        :param probs: a 2x<nmarkers> array with the AA and the BB
          homozygous probabilities.
        :type probs: numpy.darray

        :param confs: a <nmarkers> array with the confidence on the above
          probabilities.
        :type probs: numpy.darray

        """
        avid = self.kb.resolve_action_id(action)
        if not isinstance(sample, self.kb.GenotypeDataSample):
          raise ValueError('sample should be an instance of GenotypeDataSample')
        mset = sample.snpMarkersSet
        # FIXME doesn't check that probs and confs have the right dtype and size
        gdo_vid, row_index = self.add_gdo(mset.id, probs, confs, avid)
        size = 0
        sha1 = hashlib.sha1()
        s = probs.tostring();  size += len(s) ; sha1.update(s)
        s = confs.tostring();  size += len(s) ; sha1.update(s)
        conf = {
          'sample': sample,
          'path': self.make_gdo_path(mset, gdo_vid, row_index),
          'mimetype': mimetypes.GDO_TABLE,
          'sha1': sha1.hexdigest(),
          'size': size,
          }
        gds = self.kb.factory.create(self.kb.DataObject, conf).save()
        return gds

    def get_gdo(self, mset, vid, row_index, indices=None):
        table_name = self._markers_array_table_name(GDO_TABLE_NAME, mset.id)
        rows = self.kb.get_table_rows_by_indices(table_name, [row_index])
        assert len(rows) == 1
        assert rows[0]['vid'] == vid
        return self._unwrap_gdo(rows[0], indices)

    #FIXME this is the basic object, we should have some support for selections
    def get_gdo_iterator(self, mset, data_samples=None, indices = None,
                         batch_size=100):
        def get_gdo_iterator_on_list(dos):
            seen_data_samples = set([])
            for do in dos:
                # FIXME we could, in principle, handle other mimetypes too
                if do.mimetype == mimetypes.GDO_TABLE:
                    self.kb.logger.debug(do.path)
                    mset_vid, vid, row_index = self.parse_gdo_path(do.path)
                    self.kb.logger.debug('%r' % [vid, row_index])
                    if mset_vid != mset.id:
                        raise ValueError(
                            'DataObject %s map to data with a wrong SNPMarkersSet' 
                            % do.path
                        )
                    yield self.get_gdo(mset, vid, row_index, indices)
                else:
                    pass
        if data_samples is None:
            return self._get_gdo_iterator(mset.id, indices, batch_size)
        for d in data_samples:
            if d.snpMarkersSet != mset:
                raise ValueError('data_sample %s snpMarkersSet != mset' % d.id)
        ids = ','.join('%s' % ds.omero_id for ds in data_samples)
        query = 'from DataObject do where do.sample.id in (%s)' % ids
        dos = self.kb.find_all_by_query(query, None)
        return get_gdo_iterator_on_list(dos)

    def get_genotype_data_samples(self, individual, markers_set):
        """
        Syntactic sugar to simplify the looping on
        GenotypeDataSample(s) related to a specific technology (or
        markers set) connected to an individual.

        :param individual: the root individual object
        :type individual: Individual

        :param markers_set: reference SNP markers set
        :param markers_set: SNPMarkersSet

        :type return: generator of a sequence of GenotypeDataSample objects
        """
        return (d for d in self.kb.get_data_samples(individual, 
                                                    'GenotypeDataSample')
                if d.snpMarkersSet == markers_set)


    def register_vcs(self, vcs, action):
        variant_call_support.register_vcs(self.kb, vcs, action)

    def delete_vcs(self, vcs):
        variant_call_support.delete_vcs(self.kb, vcs)
        
    def get_vcs_by_label(self, label):
        return variant_call_support.get_vcs_by_label(self.kb, label)
        
    def get_vcs_by_vid(self, vid):
        return variant_call_support.get_vcs_by_vid(self.kb, vid)
        
        
            
    #----------------------------------------------------------------------
    @classmethod
    def _markers_array_table_name(klass, table_name_root, set_vid):
        assert(table_name_root in MA_TABLES)
        return '%s-%s.h5' % (table_name_root, set_vid)

    @classmethod
    def _markers_array_table_name_parse(klass, table_name):
        tag, set_vid = table_name.rsplit('.', 1)[0].rsplit('-', 1)
        if tag not in MA_TABLES:
            raise ValueError('tag %s from %s is illegal' % (tag, table_name))
        return tag, set_vid

    def _create_markers_array_table(self, table_name_root, cols_def, set_vid):
        table_name = self._markers_array_table_name(table_name_root, set_vid)
        self.kb.create_table(table_name, cols_def)
        return set_vid

    def _fill_markers_array_table(self, table_name_root, set_vid, stream,
                                  op_vid, batch_size):
        def rows_to_stream(rows):
            dtype = rows.dtype
            for r in rows:
                yield dict([(k, convert_from_numpy(r[k])) for k in dtype.names])
        def add_op_vid(stream):
            for r in stream:
                if not r.has_key('op_vid'):
                    r['op_vid'] = op_vid
                yield r
        table_name = self._markers_array_table_name(table_name_root, set_vid)
        if hasattr(stream, 'dtype'):
            stream = rows_to_stream(stream)
        return self.kb.add_table_rows_from_stream(table_name, 
                                                  add_op_vid(stream),
                                                  batch_size)
    def _unwrap_gdo(self, row, indices):
        r = {'vid': row['vid'], 'op_vid': row['op_vid']}
        p = row['probs']
        p.shape = (2, p.size/2)
        r['probs'] = p[:, indices] if indices is not None else p
        c = row['confidence']
        r['confidence'] = c[indices] if indices is not None else c
        return r

    def _get_gdo_iterator(self, set_vid, indices=None, batch_size=100):
        def iterator(stream):
          for d in stream:
            yield self._unwrap_gdo(d, indices)
        table_name = self._markers_array_table_name(GDO_TABLE_NAME, set_vid)
        return iterator(
          self.kb.get_table_rows_iterator(table_name, batch_size=batch_size)
          )
