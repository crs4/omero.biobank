# BEGIN_COPYRIGHT
# END_COPYRIGHT

import time, os

import bl.vl.utils as vu
from bl.vl.utils.ome_utils import make_unique_key, time2rtime


def assign_vid(conf):
  conf.setdefault('vid', vu.make_vid())
  return conf


def assign_vid_and_timestamp(conf, time_stamp_field='startDate'):
  conf = assign_vid(conf)
  conf.setdefault(time_stamp_field, time.time())
  return conf
