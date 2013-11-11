# BEGIN_COPYRIGHT
# END_COPYRIGHT

import hashlib
import omero
# DO NOT DELETE import omero.model, removing this import will result
# in a blocking error
import omero.model
import omero.rtypes
from bl.vl.utils import _get_env_variable


_HASHER_CLASS = hashlib.md5


def make_unique_key(*fields):
  key_string = '_'.join(map(str, fields))
  hasher = _HASHER_CLASS()
  hasher.update(key_string)
  return hasher.hexdigest().upper()


def time2rtime(t):
  return omero.rtypes.rtime(1000*t)


def rtime2time(t):
  return omero.rtypes.unwrap(t)/1000.0


def ome_hash(ome_obj):
  klass = ome_obj.__class__
  for i, k in enumerate(ome_obj.__class__.__mro__):
    if k is omero.model.IObject:
      try:
        klass = ome_obj.__class__.__mro__[i-1]
      except IndexError:
        pass
  return hash((klass.__name__, ome_obj.id._val))


def ome_host():
  return _get_env_variable('OME_HOST')


def ome_user():
  return _get_env_variable('OME_USER')


def ome_passwd():
  return _get_env_variable('OME_PASSWD')
