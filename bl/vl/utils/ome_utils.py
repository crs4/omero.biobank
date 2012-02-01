# BEGIN_COPYRIGHT
# END_COPYRIGHT

import hashlib, time
import omero.rtypes


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
