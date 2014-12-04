"""
A simple smart pointer object.

"""

from bl.vl.kb.serialize.utils import DuplicateKey
from bl.vl.kb.drivers.omero.snp_markers_set import SNPMarkersSet

class Reference(object):
    "A simple, but very specialized, pointer like object"
    known_references = {'by_ref' : {}, 'by_label' : {}, 'by_vid' : {}}

    @classmethod
    def get(cls, object_type, ref_desc):
        ref_type = ref_desc.keys()[0]
        ref_id = ref_desc[ref_type]
        if ref_id in cls.known_references[ref_type]:
            return cls.known_references[ref_type][ref_id]
        else:
            return Reference(object_type, ref_desc)

    @classmethod
    def reset(cls):
        cls.known_references = {'by_ref' : {}, 'by_label' : {}, 'by_vid' : {}}

    @classmethod
    def get_unresolved_references(cls):
        return [r for r in cls.known_references if not r.is_resolved()]

    @classmethod
    def resolve_internal_reference(cls, oid, obj):
        if cls.known_references['by_ref'].has_key(oid):
            cls.known_references['by_ref'][oid].object = obj

    @classmethod
    def resolve_external_references(cls, get_by_field):
        """Resolve external references to the KB objects requested"""
        def sort_by_type(known_references):
            by_type = {}
            for ref in known_references.values():
                by_type.setdefault(ref.object_type, {})\
                  .setdefault(ref.reference, ref)
            return by_type
        for k in ['by_label', 'by_vid']:
            srtd = sort_by_type(cls.known_references[k])
            if not srtd:
                continue
            for otype, ovalues in srtd.iteritems():
                if otype == SNPMarkersSet and k == 'by_vid':
                    known_objs = get_by_field(otype, 'markersSetVID', ovalues.keys())
                else:
                    known_objs = get_by_field(otype, k[3:], ovalues.keys())
                for o in known_objs.values():
                    o.unload()
                for i, ref in ovalues.iteritems():
                    ref.object = known_objs.get(i, None)

    def __init__(self, object_type, reference):
        self.ref_type = reference.keys()[0]
        self.reference = reference[self.ref_type]
        self.object_type = object_type
        self.object = None
        self.known_references[self.ref_type][self.reference] = self

    def is_internal(self):
        """Is this reference pointing to an object not in KB? """
        return 'by_ref' == self.ref_type

    def is_external(self):
        """Is this reference pointing to an object in KB? """
        return not self.is_internal()

    def is_resolved(self):
        """Is this reference resolved to an actual KB object? """
        return not self.object is None
