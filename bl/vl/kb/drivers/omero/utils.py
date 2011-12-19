import time

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
  return hash((ome_obj.__class__.__name__, ome_obj.id._val))
