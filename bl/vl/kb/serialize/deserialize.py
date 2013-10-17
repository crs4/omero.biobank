"""
Deserialize yaml streams to biobank objects
==========================================


"""
from bl.vl.utils import LOG_LEVELS, get_logger

from bl.vl.kb.serialize.reference import Reference
from bl.vl.kb.serialize.utils import is_a_kb_object, dewrap, sort_by_dependency
from bl.vl.kb.serialize.utils import get_attribute, get_field_descriptor
from bl.vl.kb.serialize.utils import UnknownKey, UnresolvedDependency
from bl.vl.kb.drivers.omero.data_samples import DataObject

from pygraph.classes.digraph import digraph

import yaml
import itertools as it

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
                  Reference.get(value_type, value) if not value_type.is_enum()\
                  else get_attribute(value_type, value)
            else:
                return dewrap(value_type, value)
        if self.type == DataObject:
            configuration.pop('vid')
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
            self.logger.error('Unknown key[%s] %s'
                              % (e.args, e.message))

    def itervalues(self):
        for oid, o in self.iteritems():
            yield o

    def iteritems(self):
        for t, group in self.groupbytype():
            for oid, o in group:
                yield oid, o

    def groupbytype(self):
        def fix_deps(oid):
            o = self.objects[oid].create(self.kb.factory)
            Reference.resolve_internal_reference(oid, o)
            return oid, o
        self.logger.debug('resolve_external_references start.')
        Reference.resolve_external_references(self.kb.get_by_field)
        self.logger.debug('resolve_external_references done.')        
        oids = self.get_object_oids_sorted_by_dependency()
        self.logger.debug('objects sorting done.')
        grouped = it.groupby(oids, lambda oid: self.objects[oid].type.OME_TABLE)
        for t, group in grouped:
            yield t, it.imap(fix_deps, group)
            
    def get_object_oids_sorted_by_dependency(self):
        gr = digraph()
        for i, v in self.objects.iteritems():
            self.logger.debug('adding node %s' % i)
            gr.add_node(i, attrs=[('color', v.type.OME_TABLE)])
        for i, o in self.objects.iteritems():
            for j in o.get_internal_references():
                self.logger.debug('adding edge (%s, %s)' % (j, i))
                gr.add_edge((j, i))
        return sort_by_dependency(gr, sort=True)

def deserialize_streams(kb, streams, logger=None):
    """Deserialize objects contained in streams, an iterable of yaml
       encoded stream(s) to a iterator of KB objects not yet saved in
       the biobank.

       .. code-block:: python

       with open('a.yml') as f, open('b.yml') as g:
          for o in deserialize_streams(kb, [f, g], kb.logger):
              o.save()
              print 'saved %s with id: %s' % (o, o.id)
    """
    logger = logger if logger else get_logger("deserialize_stream")
    
    limbo = ObjectsLimbo(kb, logger)
    for stream in streams:
        for ref, conf in yaml.load(stream).iteritems():
            limbo.add_object(ref, conf)
    return limbo.itervalues()

def deserialize_stream(kb, stream, logger=None):
    """Deserialize a stream of yaml encoded objects to a stream of
       KB objects not yet saved in the biobank."""
    return deserialize_streams(kb, [stream], logger)

