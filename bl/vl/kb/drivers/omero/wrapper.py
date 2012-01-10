"""

Omero Objects Wrapping
======================

.. todo::

   write docs.

"""
import omero.model as om
import omero.rtypes as ort

import bl.vl.utils as vlu
import bl.vl.utils.ome_utils as vluo
from bl.vl.kb import KBError

from utils import ome_hash


REQUIRED = 'required'
OPTIONAL = 'optional'
VID      = 'vid'
STRING   = 'string'
BOOLEAN  = 'boolean'
INT      = 'int'
LONG     = 'long'
FLOAT    = 'float'
TEXT     = 'text'
TIMESTAMP = 'timestamp'
#
SELF_TYPE = 'self-type'


WRAPPING = {TIMESTAMP : ort.rtime,
            VID       : ort.rstring,
            STRING    : ort.rstring,
            TEXT      : ort.rstring,
            FLOAT     : ort.rfloat,
            INT       : ort.rint,
            LONG      : ort.rlong,
            BOOLEAN   : ort.rbool}


def ome_wrap(v, wtype=None):
  return WRAPPING[wtype](v) if wtype else ort.wrap(v)


class CoreOmeroWrapper(object):

  OME_TABLE = None

  @classmethod
  def get_ome_type(klass):
    return getattr(om, "%sI" % klass.OME_TABLE)

  @classmethod
  def get_ome_table(klass):
    return klass.OME_TABLE

  def __init__(self, ome_obj, proxy):
    super(CoreOmeroWrapper, self).__setattr__('ome_obj', ome_obj)
    super(CoreOmeroWrapper, self).__setattr__('proxy', proxy)

  def __hash__(self):
    if not self.is_mapped():
      raise TypeError("non-persistent objects are not hashable")
    return ome_hash(self.ome_obj)

  def __eq__(self, obj):
    if type(obj) != self.__class__:
      return False
    if not self.is_mapped() or not obj.is_mapped():
      raise KBError("non-persistent objects are not comparable")
    return ((self.ome_obj.__class__.__name__, self.ome_obj.id._val) ==
            (obj.ome_obj.__class__.__name__, obj.ome_obj.id._val))

  def __ne__(self, obj):
    return not self.__eq__(obj)

  def __config__(self, ome_obj, conf):
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
      if not isinstance(v, tcode):
        raise ValueError('type(%s) != %s' % (v, tcode))
      if tcode.is_enum():
        tcode.map_enums_values(self.proxy)
      return v.ome_obj
    elif tcode in WRAPPING:
      return WRAPPING[tcode](v)
    else:
      # We cannot be here.....
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
      # We cannot be here.....
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

  def save(self):
    return self.proxy.save(self)

  def reload(self):
    self.proxy.reload_object(self)

  @property
  def id(self):
    return self.vid

  @property
  def omero_id(self):
    return self.ome_obj._id._val


class MetaWrapper(type):
  
  __KNOWN_OME_KLASSES__ = {}

  @classmethod
  def normalize_fields(klass, fields):
    nfields = {}
    for f in fields:
      nfields[f[0]] = f[1:]
    return nfields

  @classmethod
  def make_initializer(klass, base):
    def initializer(self, ome_obj=None, proxy=None):
      if not ome_obj:
        ome_obj = self.get_ome_type()()
      base.__init__(self, ome_obj=ome_obj, proxy=proxy)
    return initializer

  @classmethod
  def make_configurator(klass, base, fields):
    def configurator(self, ome_obj, conf):
      base.__config__(self, ome_obj, conf)
      conf = self.__preprocess_conf__(conf)
      for k, t in fields.iteritems():
        if k is 'vid': #
          # FIXME are we not setting this in conf?
          setattr(ome_obj, k, self.to_omero(STRING, vlu.make_vid()))
        elif k in conf:
          setattr(ome_obj, k, self.to_omero(t[0], conf[k]))
        elif t[1] is REQUIRED:
          raise ValueError('missing value for required field %s' % k)
    return configurator

  @classmethod
  def make_setter(klass, base, fields):
    def setter(self, k, v):
      if k in fields:
        setattr(self.ome_obj, k, self.to_omero(fields[k][0], v))
      else:
        base.__setattr__(self, k, v)
    return setter

  @classmethod
  def make_getter(klass, base, fields):
    def getter(self, k):
      if k in fields:
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
    attrs['__setattr__'] = MetaWrapper.make_setter(bases[0],
                                                   attrs['__fields__'])
    attrs['__getattr__'] = MetaWrapper.make_getter(bases[0],
                                                   attrs['__fields__'])
    klass = type.__new__(meta, name, bases, attrs)
    if klass.OME_TABLE:
      meta.__KNOWN_OME_KLASSES__[klass.get_ome_type()] = klass
      #-- FIXME SELF_TYPE patch
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

  def configure(self, conf):
    self.__config__(self.ome_obj, conf)
