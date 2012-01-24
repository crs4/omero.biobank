import time

import omero

import bl.vl.utils as vu
from bl.vl.utils.ome_utils import make_unique_key, time2rtime


def assign_vid(conf):
  conf.setdefault('vid', vu.make_vid())
  return conf


def assign_vid_and_timestamp(conf, time_stamp_field='startDate'):
  conf = assign_vid(conf)
  conf.setdefault(time_stamp_field, time2rtime(time.time()))
  return conf


def ome_hash(ome_obj):
  klass = ome_obj.__class__
  for i, k in enumerate(ome_obj.__class__.__mro__):
    if k is omero.model.IObject:
      try:
        klass = ome_obj.__class__.__mro__[i-1]
      except IndexError:
        pass
  return hash((klass.__name__, ome_obj.id._val))
