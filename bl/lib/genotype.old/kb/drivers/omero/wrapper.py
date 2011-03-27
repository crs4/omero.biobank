import omero
import omero.rtypes as ort
import omero_ServerErrors_ice  # magically adds exceptions to the omero module

class OmeroWrapper(object):

  OME_TABLE = None

  @classmethod
  def get_ome_type(klass):
    return getattr(om, "%sI" % klass.OME_TABLE)

  def __init__(self, ome_obj):
    super(OmeroWrapper, self).__setattr__("ome_obj", ome_obj)

  def __getattr__(self, name):
    return ort.unwrap(getattr(self.ome_obj, name))

  # WARNING: the 'wrap' function performs only basic type
  # conversions. Override this when more sophisticated conversions are
  # required (e.g., timestamps or computed results)
  def __setattr__(self, name, value):
    return setattr(self.ome_obj, name, ort.wrap(value))

  @property
  def id(self):
    return self.vid


