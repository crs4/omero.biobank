"""
Knoledge base deserializer
==========================

"""
from bl.vl.utils import LOG_LEVELS, get_logger

from bl.vl.serialize.reference import Reference
from bl.vl.serialize.utils import is_a_kb_object, dewrap, sort_by_dependency
from bl.vl.serialize.utils import get_attribute, get_field_descriptor
from bl.vl.serialize.utils import UnknownKey, UnresolvedDependency

from pygraph.classes.digraph import digraph

import yaml

class ObjectProxy(object):
    """(Almost) all you need to known to create an object"""
    def __init__(self, otype, conf):
        "Object configuration container."
        self.type = otype
        self.config = self.create_config(conf)
        self.object = None

    def create(self, factory):
        if not self.object:
            self.object = factory.create(self.type, self.get_configuration())
            if self.config.has_key('vid'):
                # very special case. over-rides automatic vid-creation
                self.object.__setattr__('vid', self.config['vid'])
        return self.object
            
    def create_config(self, configuration):
        def convert_value(key, value):
            value_type, value_use = get_field_descriptor(self.type, key)
            if is_a_kb_object(value_type):
                return \
                  Reference(value_type, value) if not value_type.is_enum()\
                  else get_attribute(value_type, value)
            else:
                return dewrap(value_type, value)
        return dict([(k, convert_value(k, configuration[k]))
                    for k in configuration])

    def get_configuration(self):
        def fix_ref(k, v):
            if not type(v) == Reference:
                return v
            else:
                if not (v.object and v.object.is_mapped()):
                    print 'k:{} v:{}'.format(k, v)
                    raise UnresolvedDependency(self, k)
                return v.object
        return dict([(k, fix_ref(k, v)) for k, v in self.config.iteritems()])

    def get_internal_references(self):
        def is_iref(v):
            return type(v) == Reference and v.is_internal()
        return [v.reference for v in self.config.values() if is_iref(v)]

class ObjectsLimbo(object):
    """
    FIXME
    """
    def __init__(self, kb, logger):
        self.objects = {}
        self.kb = kb
        self.logger = logger
        # FIXME, this is just an hack to avoid unexpected references
        Reference.reset()

    def add_object(self, oid, description):
        try:
            otype = get_attribute(self.kb, description['type'])
            oconf = description['configuration']
            self.objects[oid] = ObjectProxy(otype, oconf)
        except UnknownKey, e:
            self.logger.error('Unknown key %s in %s from %r'
                              % (e.key, e.obj, description))

    def itervalues(self):
        for oid, o in self.iteritems():
            yield o

    def iteritems(self):
        Reference.resolve_external_references(self.kb.get_by_field)
        oids = self.get_object_oids_sorted_by_dependency()
        for oid in oids:
            o = self.objects[oid].create(self.kb.factory)
            Reference.resolve_internal_reference(oid, o)
            yield oid, o
            
    def get_object_oids_sorted_by_dependency(self):
        gr = digraph()
        for i in self.objects:
            self.logger.debug('adding node %s' % i)
            gr.add_node(i)
        for i, o in self.objects.iteritems():
            for j in o.get_internal_references():
                self.logger.debug('adding edge (%s, %s)' % (j, i))
                gr.add_edge((j, i))
        return sort_by_dependency(gr)

def deserialize_stream(kb, stream, logger=None):
    """Deserialize a stream of yaml encoded objects to a stream of
    (oref, KB object) pairs."""
    logger = logger if logger \
      else get_logger("deserialize_stream")
    
    limbo = ObjectsLimbo(kb, logger)
    for ref, conf in yaml.load(stream).iteritems():
        limbo.add_object(ref, conf)
    return limbo.itervalues()

