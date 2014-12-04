# BEGIN_COPYRIGHT
# END_COPYRIGHT

import omero.model as om
import omero.rtypes as ort

import bl.vl.utils as vlu
import bl.vl.utils.ome_utils as vluo
from bl.vl.kb import KBError
from bl.vl.utils.graph import graph_driver
from bl.vl.kb.serialize.serializer import Serializer

REQUIRED = 'required'
OPTIONAL = 'optional'
VID = 'vid'
STRING = 'string'
BOOLEAN = 'boolean'
INT = 'int'
LONG = 'long'
FLOAT = 'float'
TEXT = 'text'
TIMESTAMP = 'timestamp'
SELF_TYPE = 'self-type'

def safe_rstring(text):
  text = str(text) if type(text) == unicode else text
  return ort.rstring(text)

WRAPPING = {
  TIMESTAMP: vluo.time2rtime,
  VID: safe_rstring,
  STRING: safe_rstring,
  TEXT: safe_rstring,
  FLOAT: ort.rfloat,
  INT: ort.rint,
  LONG: ort.rlong,
  BOOLEAN: ort.rbool,
  }


def ome_wrap(v, wtype=None):
  v = str(v) if type(v) == unicode else v
  return WRAPPING[wtype](v) if wtype else ort.wrap(v)


class CoreOmeroWrapper(object):

  OME_TABLE = None
  __do_not_serialize__ = []

  @classmethod
  def get_ome_type(cls):
    return getattr(om, "%sI" % cls.OME_TABLE)

  @classmethod
  def get_ome_table(cls):
    return cls.OME_TABLE

  def __init__(self, ome_obj, proxy):
    super(CoreOmeroWrapper, self).__setattr__('ome_obj', ome_obj)
    super(CoreOmeroWrapper, self).__setattr__('proxy', proxy)

  def __hash__(self):
    if not self.is_mapped():
      raise TypeError("non-persistent objects are not hashable")
    return vluo.ome_hash(self.ome_obj)

  def __eq__(self, obj):
    if type(obj) != self.__class__:
      return False
    # if objects have a VID field we can compare them even if they are not mapped
    if hasattr(self, 'id'):
      self_id = self.id
      obj_id = obj.id
    # otherwise we need the ID field of the ome_obj and objects must be mapped
    else:
      if not self.is_mapped() or not obj.is_mapped():
        raise KBError("non-persistent objects without VID field are not comparable")
      self_id = self.ome_obj.id._val
      obj_id = self.ome_obj.id._val
    return ((self.ome_obj.__class__.__name__, self_id) ==
            (obj.ome_obj.__class__.__name__, obj_id))

  def __ne__(self, obj):
    return not self.__eq__(obj)

  def __config__(self, ome_obj, conf):
    pass

  def __to_conf__(self, ome_obj, conf):
    pass
    
  def bare_getattr(self, name):
    return super(CoreOmeroWrapper, self).__getattribute__(name)

  def bare_setattr(self, name, v):
    super(CoreOmeroWrapper, self).__setattr__(name, v)

  def __getattr__(self, name):
    if hasattr(self, name):
      return getattr(self, name)
    else:
      raise AttributeError('object %s has no attribute %s' %
                           (self.__class__.__name__, name))

  def __setattr__(self, name, v):
    if hasattr(self, name):
      super(CoreOmeroWrapper, self).__setattr__(name, v)
    else:
      raise AttributeError('object %s has no attribute %s' %
                           (self.__class__.__name__, name))

  def enum_label(self):
    if not self.is_enum():
      raise ValueError('%s is not an enum' % self)
    return self.ome_obj.value._val

  def to_omero(self, tcode, v):
    if isinstance(tcode, type):
      if v is None:
        return v
      if not isinstance(v, tcode):
        raise ValueError('type(%s) != %s' % (v, tcode))
      if tcode.is_enum():
        tcode.map_enums_values(self.proxy)
      return v.ome_obj
    elif tcode in WRAPPING:
      return WRAPPING[tcode](v)
    else:
      raise ValueError('illegal tcode value: %s' % tcode)

  def from_omero(self, tcode, v):
    if isinstance(tcode, type):
      o = self.proxy.factory.wrap(v)
      if not isinstance(o, tcode):
        raise ValueError('inconsistent type result type(%s) for %s' %
                         (type(o), tcode))
      return o
    elif tcode is TIMESTAMP:
      return vluo.rtime2time(ort.unwrap(v))
    elif tcode in WRAPPING:
      return ort.unwrap(v)
    else:
      raise ValueError('illegal tcode value: %s' % tcode)

  def is_mapped(self):
    return self.ome_obj.id is not None

  def is_loaded(self):
    return self.is_mapped() and self.ome_obj.loaded

  def is_upcastable(self):
    pass

  def upcast(self):
    pass

  def unload(self):
    self.ome_obj.unload()

  def save(self, move_to_common_space=False):
    return self.proxy.save(self, move_to_common_space)

  def in_current_sandbox(self):
    return self.proxy.in_current_sandbox(self)

  def serialize(self, engine, shallow=False):
    if not isinstance(engine, Serializer):
        raise ValueError('%s is not a Serializer' % engine)
    if not self.is_loaded() and self.is_mapped():
        self.reload()
    if engine.has_seen(self.id):
        return
    conf = self.to_conf()
    # Remove unique keys from config
    for field in self.__do_not_serialize__:
        conf.pop(field)
    for k in conf:
        if isinstance(conf[k], CoreOmeroWrapper):
            if conf[k].is_enum():
                if not conf[k].is_loaded():
                    conf[k].reload()
                conf[k] = conf[k].enum_label()
            else:
                if shallow:
                    conf[k] = engine.by_vid(conf[k].id)
                else:
                    if conf[k].in_current_sandbox():
                        conf[k].serialize(engine)                
                        conf[k] = engine.by_ref(conf[k].id)
                    else:
                        conf[k] = engine.by_vid(conf[k].id)
    engine.serialize(self.id, self.get_ome_table(), conf, vid=self.vid)
    engine.register(self.id)
  
  def reload(self):
    self.proxy.reload_object(self)

  @property
  def id(self):
    #return 'DB_MAGIC_NUMBER:%s:%s' % (self.get_ome_table(), self.omero_id)
    return self.vid

  @property
  def omero_id(self):
    return self.ome_obj._id._val
    
  def get_namespace(self):
    close_connection = False
    if not self.proxy.current_session:
      self.proxy.connect()
      close_connection = True
    namespace, _ = self.proxy.get_current_group()
    if close_connection:
      self.proxy.disconnect()
    return namespace


class MetaWrapper(type):
  
  __KNOWN_OME_KLASSES__ = {}

  @classmethod
  def normalize_fields(cls, fields):
    nfields = {}
    for f in fields:
      nfields[f[0]] = f[1:]
    return nfields

  @classmethod
  def make_initializer(cls, base):
    def initializer(self, ome_obj=None, proxy=None):
      if not ome_obj:
        ome_obj = self.get_ome_type()()
      base.__init__(self, ome_obj=ome_obj, proxy=proxy)
    return initializer

  @classmethod
  def make_configurator(cls, base, fields):
    def configurator(self, ome_obj, conf):
      base.__config__(self, ome_obj, conf)
      for k, t in fields.iteritems():
        if k in conf:
          setattr(ome_obj, k, self.to_omero(t[0], conf[k]))
        elif t[1] is REQUIRED:
          raise ValueError('missing value for required field %s' % k)
    return configurator

  @classmethod
  def make_to_conf(cls, base, fields):
    def to_conf(self, ome_obj, conf):
      base.__to_conf__(self, ome_obj, conf)
      for k, t in fields.iteritems():
        v = getattr(ome_obj, k)
        if v != None:
          conf[k] = self.from_omero(t[0], v)
    return to_conf

  @classmethod
  def make_setter(cls, base, fields):
    def setter(self, k, v):
      if k in fields:
        setattr(self.ome_obj, k, self.to_omero(fields[k][0], v))
        self.__update_constraints__()
      else:
        base.__setattr__(self, k, v)
    return setter

  @classmethod
  def make_getter(cls, base, fields):
    def getter(self, k):
      if k in fields:
        if not self.is_loaded() and self.is_mapped():
          self.reload()
        v = getattr(self.ome_obj, k)
        if v is None:
          return None
        if isinstance(fields[k][0], type):
          cached_v = self.proxy.get_from_cache(v)
          if cached_v:
            return cached_v
          elif not v.loaded:
            v = self.proxy.ome_operation('getQueryService', 'find',
                                         v.__class__.__name__[:-1],
                                         v.id.val)
        return self.from_omero(fields[k][0], v) if v else None
      else:
        return base.__getattr__(self, k)
    return getter

  def __new__(meta, name, bases, attrs):
    if not attrs.has_key('__fields__'):
      attrs['__fields__'] = []
    attrs['__fields__'] = MetaWrapper.normalize_fields(attrs['__fields__'])
    attrs['__init__']   = MetaWrapper.make_initializer(bases[0])
    attrs['__config__'] = MetaWrapper.make_configurator(bases[0],
                                                        attrs['__fields__'])
    attrs['__to_conf__'] = MetaWrapper.make_to_conf(bases[0],
                                                    attrs['__fields__'])
    attrs['__setattr__'] = MetaWrapper.make_setter(bases[0],
                                                   attrs['__fields__'])
    attrs['__getattr__'] = MetaWrapper.make_getter(bases[0],
                                                   attrs['__fields__'])
    klass = type.__new__(meta, name, bases, attrs)
    if klass.OME_TABLE:
      meta.__KNOWN_OME_KLASSES__[klass.get_ome_type()] = klass
      fields = attrs['__fields__']
      for k in fields:
        if fields[k][0] == SELF_TYPE:
          fields[k] = (klass,) + fields[k][1:]
    if klass.is_enum():
      enums = []
      for l in klass.__enums__:
        o = klass(ome_obj=None, proxy=None)
        o.ome_obj.value = ort.wrap(l)
        enums.append(o)
        setattr(klass, l, o)
      klass.__enums__ = enums
    return klass


class ObjectFactory(object):
  
  def __init__(self, proxy):
    self.proxy = proxy

  def wrap(self, ome_obj):
    cached = self.proxy.get_from_cache(ome_obj)
    if cached is not None:
      return cached
    else:
      klass = MetaWrapper.__KNOWN_OME_KLASSES__[type(ome_obj)]
      o = klass(ome_obj=ome_obj, proxy=self.proxy)
      self.proxy.store_to_cache(o)
      return o

  def create(self, klass, conf):
    o = klass(ome_obj=None, proxy=self.proxy)
    o.configure(conf)
    return o


class OmeroWrapper(CoreOmeroWrapper):
  """
  All kb.drivers.omero classes should derive from this class.
  """
  __enums__ = []
  __fields__ = []
  __metaclass__ = MetaWrapper

  OME_TABLE = None

  @classmethod
  def is_enum(klass):
    return len(klass.__enums__) > 0

  @classmethod
  def map_enums_values(klass, proxy):
    assert klass.is_enum()
    for o in klass.__enums__:
      if not o.is_mapped():
        proxy.update_by_example(o)

  def __preprocess_conf__(self, conf):
    return conf

  def __update_constraints__(self):
    pass

  def __dump_to_graph__(self, is_update):
    relationships = {
      self.proxy.DataCollectionItem: 'dataSample',
      self.proxy.VesselsCollectionItem: 'vessel',
    }
    if hasattr(self, 'action'):
      if not is_update:
        self.proxy.dt.create_node(self)
      self.action.reload()
      if hasattr(self.action, 'target'):
        if type(self.action.target) not in relationships:
          self.proxy.dt.create_edge(self.action, self.action.target, self)
        else:
          source = getattr(self.action.target, relationships[type(self.action.target)])
          self.proxy.dt.create_edge(self.action, source, self)

  def __precleanup__(self):
    pass

  def __cleanup__(self):
    if hasattr(self, 'action'):
      self.proxy.dt.destroy_node(self)
      # also delete the edge that connects the object to its source
      if hasattr(self.action, 'target'):
        self.proxy.dt.destroy_edge(self, self.action.target)

  def configure(self, conf):
    conf = self.__preprocess_conf__(conf)
    self.__config__(self.ome_obj, conf)

  def to_conf(self):
    conf = {}
    self.__to_conf__(self.ome_obj, conf)
    return conf

  def get_owner(self):
    return self.proxy.get_object_owner(self)

  def get_group(self):
    return self.proxy.get_object_group(self)
