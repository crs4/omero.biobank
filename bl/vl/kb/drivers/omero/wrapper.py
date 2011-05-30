import omero.model as om
import omero.rtypes as ort

import bl.vl.utils as vlu
import bl.vl.utils.ome_utils as vluo

"""

Expected Usage
--------------


class Action(OmeroWrapper):
  __fields__ = []

factory.create(Action, {'vid' : ort.rstring(vluo.make_vid()),
                          'beginTime' : vluo.time2rtime(time.time())})

will use

a = Action(ome_obj=None, proxy, {'vid' : ort.rstring(vluo.make_vid()),
                                 'beginTime' : vluo.time2rtime(time.time())})

factory.wrap(ome_obj)

will decide to use Action and then use

a = Action(ome_obj=ome_obj, proxy, None)

"""

REQUIRED = 'required'
OPTIONAL = 'optional'
VID      = 'vid'
STRING   = 'string'
BOOLEAN  = 'boolean'
INT      = 'int'
FLOAT    = 'float'
TEXT     = 'text'
TIMESTAMP = 'timestamp'


WRAPPING = {TIMESTAMP : ort.rtime,
            VID       : ort.rstring,
            STRING    : ort.rstring,
            FLOAT     : ort.rfloat,
            INT       : ort.rint,
            BOOLEAN   : ort.rbool}


def ome_wrap(self, v, wtype=None):
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

  def __config__(self, ome_obj, conf):
    pass

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

  def to_omero(self, tcode, v):
    if type(tcode) == type:
      if not isinstance(v, tcode):
        raise ValueError('type(%s) != %s' % (v, tcode))
      return v.ome_obj
    elif tcode in WRAPPING:
      return WRAPPING[tcode](v)
    else:
      # We cannot be here.....
      raise ValueError('illegal tcode value: %s' % tcode)

  def from_omero(self, tcode, v):
    if type(tcode) == type:
      o = self.proxy.object_factory.wrap(v)
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

  def is_upcastable(self):
    pass

  def upcast(self):
    pass

  def unload(self):
    self.ome_obj.unload()

  def save(self):
    return self.proxy.save(self)

  def load(self):
    pass

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
        return self.from_omero(fields[k][0], getattr(self.ome_obj, k))
      else:
        return base.__getattr__(self, k)
    return getter

  def __new__(meta, name, bases, attrs):
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
    return klass


class ObjectFactory(object):
  def __init__(self, proxy):
    self.proxy = proxy

  def wrap(self, ome_obj):
    klass = MetaWrapper.__KNOWN_OME_KLASSES__[type(ome_obj)]
    return klass(ome_obj=ome_obj, proxy=self.proxy)

  def create(self, klass, conf):
    o = klass(ome_obj=None, proxy=self.proxy)
    o.configure(conf)
    return o

class OmeroWrapper(CoreOmeroWrapper):
  """
  All kb.drivers.omero classes should derive from this class.
  """

  __fields__ = []
  __metaclass__ = MetaWrapper

  OME_TABLE = None

  def __preprocess_conf__(self, conf):
    return conf

  def configure(self, conf):
    self.__config__(self.ome_obj, conf)


