import wrapper as wp

from action import Action, assing_vid_and_timestamp

class DataSampleStatus(wp.OmeroWrapper):
  OME_TABLE = 'DataSampleStatus'

  __enums__ = ["UNKNOWN", "DESTROYED", "CORRUPTED", "USABLE"]


class DataSample(wp.OmeroWrapper):
  OME_TABLE = 'DataSample'
  __fields__ = [('vid', wp.VID, wp.REQUIRED),
                ('creationDate', wp.TIMESTAMP, wp.REQUIRED),
                ('status', DataSampleStatus, wp.REQUIRED),
                ('action', Action, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    return assing_vid_and_timestamp(conf, time_stamp_field='creationDate')


class DataObject(wp.OmeroWrapper):
  OME_TABLE = 'DataObject'
  __fields__ = [('sample', DataSample, wp.REQUIRED)]




