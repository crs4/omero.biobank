"""

Variant Call Support
====================

The VCS data model is somewhat tricky. Its information is stored in an
omero object derived from DataSample, and the associated DataObject.
The client-side python object merges data coming from both. As a
result, get/register operations cannot be done directly but should be
mediated by kb.

.. code-block:: python

   vcs1 = get_vcs_by_label(kb, 'label1') 
   vcs2 = get_vcs_by_label(kb, 'label2')   
   vcs3 = vcs1.union(vcs2)
   vcs3.label = 'label1+label2'
   register_vcs(kb, vcs3)
   
  
"""
import omero.model as om
import omero.rtypes as ort


import numpy as np
import uuid
import json
import hashlib

import wrapper as wp

from bl.vl.kb import mimetypes
from bl.vl.kb.drivers.omero.data_samples import DataSample
from bl.vl.kb.drivers.omero.sequencing import ReferenceGenome

import bl.vl.utils as vlu
import bl.vl.utils.np_ext as np_ext


def get_vcs_by_label(kb, label):
    "Recover a VariantCallSupport definition by label"
    vcs = kb.get_by_label(VariantCallSupport, label)
    return None if vcs is None else _restore_data(kb, vcs)

def get_vcs_by_vid(kb, vid):
    "Recover a VariantCallSupport definition by vid"
    vcs = kb.get_by_vid(VariantCallSupport, vid)
    return None if vcs is None else _restore_data(kb, vcs)    

def _restore_data(kb, vcs):
    # pylint: disable=C0111    
    dos = kb.get_data_objects(vcs)
    if len(dos) == 0:
        return vcs # empty vcs
    nodes, fields = _get_vcs_data(kb, dos)
    vcs.define_support(nodes)
    vcs.define_fields(fields)
    return vcs

def _make_table_name(vcs, tag):
    return '%s:%s.h5' % (vcs.id, tag)

def _pack_in_path(table_names):
    return json.dumps(table_names)    

def _unpack_path(path):
    return json.loads(path)
    
def _get_vcs_data(kb, dos):
    # pylint: disable=C0111
    for do in dos:
        do.reload()
        if do.mimetype == mimetypes.VCS_TABLES:
            table_names = _unpack_path(do.path)
            nodes = kb.read_whole_table(table_names['support']['nodes'])
            fields = {}
            for name, table_name in table_names['fields'].iteritems():
                fields[name] = kb.read_whole_table(table_name)
            return nodes, fields
    else:
        raise RuntimeError('cannot find data fields')

def _save_vcs_data(kb, vcs):
    # pylint: disable=C0111
    nodes = vcs.get_nodes()
    fields = vcs.get_fields()
    table_names = {'support': {}, 'fields' : {}}
    sha1 = hashlib.sha1()
    size = 0
    table_name = _make_table_name(vcs, 'support.nodes')
    table_names['support']['nodes'] = table_name
    kb.store_as_a_table(table_name, nodes)
    sha1.update(nodes.data)
    size += len(nodes.data)
    for k in fields:
        table_name = _make_table_name(vcs, 'fields.%s' % k)
        table_names['fields'][k] = table_name        
        kb.store_as_a_table(table_name, fields[k])
        sha1.update(fields[k].data)
        size += len(fields[k].data)
    conf = {'sample' : vcs,
            'mimetype' : mimetypes.VCS_TABLES, 
            'path' : _pack_in_path(table_names),
            'sha1' : sha1.hexdigest(),
            'size' : size,
            }
    return kb.factory.create(kb.DataObject, conf)

def register_vcs(kb, vcs, action):
    "Creates a permanent copy of a VariantCallSupport"
    vcs.save()
    return _save_vcs_data(kb, vcs)


def delete_vcs(kb, vcs):
    "Deletes vcs from permanent storage"
    dos = kb.get_data_objects(vcs)
    if len(dos) > 0:
        _delete_data(kb, vcs)
    kb.delete(vcs)

def _delete_data(kb, vcs):
    for do in dos:
        do.reload()
        if do.mimetype == mimetypes.VCS_TABLES:
            table_names = _unpack_path(do.path)
            kb.delete_table(table_names['support']['nodes'])
            for table_name in table_names['fields'].values():
                kb.delete_table(table_name)
    else:
        raise RuntimeError('cannot find data fields')
    

VID_SIZE = vlu.DEFAULT_VID_LEN


class  VariantCallSupport(DataSample):
    """
    Describes location and type of variant calls wrt a given reference genome.
    
    
    """
    OME_TABLE = 'VariantCallSupport'
    __fields__ = [('referenceGenome', ReferenceGenome, wp.REQUIRED)]

    CHROMOSOME_SCALE = 10**12 # this should allow up to 10**7 chromosomes

    NODES_DTYPE = np.dtype([('chrom', '<i4'), ('pos', '<i8')])
    ATTR_ORIGIN_DTYPE = np.dtype([('index', '<i4'), 
                                  ('vid', '|S%d' % VID_SIZE), ('vpos', '<i8')])
    #ATTR_VC_DTYPE  = np.dtype([('ref', '|S%d' % VID_SIZE), ('vpos', '<i8')]) 

    def save(self):
        super(VariantCallSupport, self).save()

    def __cleanup__(self):
        dos = self.proxy.get_data_objects(self)
        if len(dos) > 0:
            _delete_data(self.proxy, self)
        
    def __len__(self):
        return len(self.get_nodes())

    def get_nodes(self):
        """
        Returns positions known to this support.

        result is a numpy array of dtype NODES_DTYPE
        """
        return np.array([], dtype=self.NODES_DTYPE) \
          if not hasattr(self, 'nodes') else self.bare_getattr('nodes')

    def get_field(self, name):
        """
        Returns field name.

        result is None if there is no field with that name. A field
        is a numpy record array with, at least, a 'index' column that
        links records to their supporting node.
        """
        return (self.get_fields()).get(name)

    def get_fields(self):
        """
        Returns all fields known by this support.

        result is dictionary indexed by field name. Each field is a
        numpy record array with, at least, a 'index' column that links
        records to their supporting node.
        """
        return {} if not hasattr(self, 'fields') \
                  else self.bare_getattr('fields')

    def define_support(self, nodes):
        """
        vcs.define_support(np.array(data, dtype=vcs.NODES_DTYPE))

        """
        
        if (type(nodes) is not np.ndarray 
            or nodes.dtype is not self.NODES_DTYPE):
            raise ValueError('nodes is not a compatible numpy array')
        self._check_strictly_increasing(nodes)
        self._define_support(nodes)
        
    def define_field(self, name, field):
        """
        vcs.define_field('origin', np.array(data, 
                                            dtype=[('index', '<i4'), ...]))
        """
        if self.nodes is None:
            raise RuntimeError('no support, cannot associate field')
        if (field['index'].min() < 0  
            or field['index'].max() >= len(self.get_nodes())):
            raise ValueError('bad field: index out of range')
        self._check_strictly_increasing(field)        
        self._define_field(name, field)

    def define_fields(self, fields):
        """
        vcs.define_fields({'origin': .., 'snp': ..., 'indel':, ...})
        """
        for k in fields:
            self.define_field(k, fields[k])

    def selection(self, gc_range):
        """
        Extract a subregion identified by gc_range.
        
        gc_range is a two elements tuple that represents the
        sub-region extrema. Each element is a tuple (chromosome,
        position), where chromosome is an int in the range [1,
        nchromosomes], and pos is a positive int. The region selected
        goes from gc_range[0] included to gc_range[1] excluded.
        """
        nodes = self.get_nodes()
        fields = self.get_fields()
        beg, end = [self._get_gpos(x[0], x[1]) for x in gc_range]
        gpos = self._get_gpos(nodes['chrom'], nodes['pos'])
        sel = (gpos >= beg) & (gpos < end)
        mapped_index = sel.cumsum() - 1 # 
        nfields = {}
        for k in fields:
            nfields[k] = self._fix_field_index(fields[k], sel, mapped_index)
        return self._clone_structure(nodes[sel], nfields)

    def union(self, other):
        """
        return the union between self and other.
        """
        self._check_other(other)
        s_isct, _, o_isct, o_sel = self._get_union_selectors(other)
        nodes = np.hstack([self.get_nodes(), other.get_nodes()[o_sel]])
        #shuffle = np.lexsort([nodes['pos'], nodes['chrom']])
        shuffle = np.argsort(nodes)
        return self._clone_structure(nodes[shuffle],
                        self._unite_fields(other, shuffle, s_isct, 
                                           o_isct, o_sel))
        
    def _get_union_selectors(self, other):
        # pylint: disable=C0111
        self_nodes = self.get_nodes()
        other_nodes = other.get_nodes()
        self_isct, other_isct = np_ext.index_intersect(self_nodes, other_nodes)
        self_sel = np.ones((len(self_nodes),), dtype=np.bool)
        other_sel = np.ones((len(other_nodes),), dtype=np.bool)
        other_sel[other_isct] = False
        return self_isct, self_sel, other_isct, other_sel

    def _unite_fields(self, other, shuffle, s_isct, o_isct, o_sel):
        # pylint: disable=C0111        
        self_nodes = self.get_nodes()
        other_nodes = other.get_nodes()
        self_fields = self.get_fields()
        other_fields = other.get_fields()
        s_keys, o_keys = (set(fs) for fs in [self_fields, other_fields])
        o_map = (o_sel.cumsum() - 1) + len(self_nodes)
        inv_shuffle = shuffle.argsort()
        o_map[o_isct] = s_isct
        o_map = inv_shuffle[o_map]
        fields = {}
        for k in s_keys.union(o_keys):
            chunks = []
            if k in s_keys:
                chunks.append(self._fix_field_index(self_fields[k], 
                                                    None, inv_shuffle))
            if k in o_keys:
                chunks.append(self._fix_field_index(other_fields[k], 
                                                    None, o_map))
            fields[k] = self._kill_duplicates(np.hstack(chunks))
        return fields

    def intersection(self, other):
        """
        return the intersection between self and other.
        """
        self._check_other(other)
        s_icst, s_sel, o_isct, o_sel = self._get_intersection_selectors(other)
        return self._clone_structure(self.get_nodes()[s_sel],
                    self._intersection_fields(other, s_icst, s_sel, 
                                              o_isct, o_sel))
    def _get_intersection_selectors(self, other):
        # pylint: disable=C0111        
        self_nodes = self.get_nodes()
        other_nodes = other.get_nodes()
        self_isct, other_isct = np_ext.index_intersect(self_nodes, other_nodes)
        self_sel = np.zeros((len(self_nodes),), dtype=np.bool)
        self_sel[self_isct] = True
        other_sel = np.zeros((len(other_nodes),), dtype=np.bool)
        other_sel[other_isct] = True
        return self_isct, self_sel, other_isct, other_sel
        
    def _intersection_fields(self, other, s_isct, s_sel, o_isct, o_sel):
        # pylint: disable=C0111        
        other_nodes = other.get_nodes()        
        self_fields = self.get_fields()
        other_fields = other.get_fields()
        s_keys, o_keys = (set(fs) for fs in [self_fields, other_fields])
        s_map = s_sel.cumsum() - 1
        o_map = np.arange(0, len(other_nodes))
        o_map[o_isct] = s_map[s_isct]
        fields = {}
        for k in s_keys.union(o_keys):
            chunks = []
            if k in s_keys:
                chunks.append(self._fix_field_index(self_fields[k], 
                                                    s_sel, s_map))
            if k in o_keys:
                chunks.append(self._fix_field_index(other_fields[k], 
                                                    o_sel, o_map))
            fields[k] = self._kill_duplicates(np.hstack(chunks))
        return fields

    def complement(self, other):
        """
        return the relative complement of other in self.
        
        """
        self._check_other(other)
        s_icst, s_sel = self._get_complement_selectors(other)
        return self._clone_structure(self.get_nodes()[s_sel],
                    self._complement_fields(s_icst, s_sel))

    def _get_complement_selectors(self, other):
        # pylint: disable=C0111        
        self_nodes = self.get_nodes()
        other_nodes = other.get_nodes()
        self_isct, other_isct = np_ext.index_intersect(self_nodes, other_nodes)
        self_sel = np.ones((len(self_nodes),), dtype=np.bool)
        self_sel[self_isct] = False
        self_isct = np.arange(0, len(self_nodes))[self_sel]
        return self_isct, self_sel
        
    def _complement_fields(self, s_isct, s_sel):
        # pylint: disable=C0111        
        self_fields = self.get_fields()
        s_map = s_sel.cumsum() - 1
        fields = {}
        for k in self_fields:
            fields[k] = self._fix_field_index(self_fields[k], s_sel, s_map)
        return fields
        
    #------------- utility functions below here ----------------------
    # pylint: disable=C0111
    @staticmethod
    def _create_label():
        return uuid.uuid4().hex

    def _define_support(self, nodes):
        self.bare_setattr('nodes', nodes)

    @classmethod            
    def _get_gpos(cls, chrom, pos):
        return chrom*cls.CHROMOSOME_SCALE + pos

    def _define_field(self, name, field):
        if hasattr(self, 'fields'):
            self.bare_getattr('fields')[name] = field
        else:
            self.bare_setattr('fields', {name : field})

    @staticmethod    
    def _fix_field_index(ofield, selector, mapped_index):
        if selector is None:
            field = ofield.copy()
        else:
            field = ofield[selector[ofield['index']]]
        field['index'] = mapped_index[field['index']]
        return field

    @staticmethod        
    def _kill_duplicates(records):
        rsorted = records[records.argsort()]
        return rsorted[np.hstack([[True], rsorted[1:] != rsorted[:-1]])]

    def _clone_structure(self, support=None, fields=None):
        conf = self.to_conf()
        conf['label'] = self._create_label()
        other = self.proxy.factory.create(self.__class__, conf)
        if support is not None:
            other._define_support(support)
        if fields is not None:
            for k in fields:
                other._define_field(k, fields[k])
        return other
    
    @staticmethod        
    def _check_strictly_increasing(records):
        #idx = np.lexsort([records['pos'], records['chrom']])
        idx = records.argsort()
        if not (np.alltrue(idx == np.arange(0, len(records)))
                and np.alltrue(records[1:] != records[:-1])):
            raise ValueError('records is not monotonically increasing')

    def _check_other(self, other):
        if not isinstance(other, type(self)):
            raise ValueError('other is of an incompatible type')
        if other.nodes is None:
            raise ValueError('other.nodes is None')
        if self.referenceGenome != other.referenceGenome:
            raise ValueError('other has a different referenceGenome')
            
