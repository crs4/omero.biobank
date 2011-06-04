import bl.vl.utils as vu
from bl.vl.utils.ome_utils import make_unique_key, time2rtime

import time

def assign_vid(conf):
  if not 'vid' in conf:
    conf['vid'] = vu.make_vid()
  return conf

def assign_vid_and_timestamp(conf, time_stamp_field='startDate'):
  conf = assign_vid(conf)
  if not time_stamp_field in conf:
    conf[time_stamp_field] = time2rtime(time.time())
  return conf
