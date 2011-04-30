import omero.model as om
import omero.rtypes as ort

# Note: OmeroWrapper cannot add a vid attribute to ome_obj, because this
#       will break the wrapping of standard omero objects
class OmeroWrapper(object):

  OME_TABLE = None

  @classmethod
  def get_ome_type(klass):
    return getattr(om, "%sI" % klass.OME_TABLE)

  @classmethod
  def get_ome_table(klass):
    return klass.OME_TABLE

  def __init__(self, ome_obj, **kw):
    self.__set_proxy__(kw.get('proxy', None))
    super(OmeroWrapper, self).__setattr__("ome_obj", ome_obj)

  def __set_proxy__(self, proxy):
    super(OmeroWrapper, self).__setattr__("proxy", proxy)

  def __get_proxy__(self):
    return super(OmeroWrapper, self).__getattribute__("proxy")

  def __get_if_needed__(self, name):
    proxy = self.__get_proxy__()
    if proxy and not self.ome_obj.loaded:
      o = proxy.ome_operation("getQueryService", "get", self.OME_TABLE,
                              self.ome_obj.id._val)
      super(OmeroWrapper, self).__setattr__("ome_obj", o)

  def __handle_validation_errors__(self):
    raise NotImplementedError("OmeroWrapper cannot handle validation errors")

  def __getattr__(self, name):
    self.__get_if_needed__(name)
    return ort.unwrap(getattr(self.ome_obj, name))

  # WARNING: the 'wrap' function performs only basic type
  # conversions. Override this when more sophisticated conversions are
  # required (e.g., timestamps or computed results)
  def __setattr__(self, name, value):
    if hasattr(value, "ome_obj"):
      return setattr(self.ome_obj, name, value.ome_obj)
    else:
      return setattr(self.ome_obj, name, ort.wrap(value))

  @property
  def id(self):
    return self.vid

  @property
  def omero_id(self):
    return self.ome_obj._id._val


