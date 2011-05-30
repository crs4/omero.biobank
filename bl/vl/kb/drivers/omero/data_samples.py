import wrapper as wp

from action import Action

class DataSampleStatus(wp.OmeroWrapper):
  OME_TABLE = 'DataSampleStatus'
  __fields__ = []


class DataSample(wp.OmeroWrapper):
  OME_TABLE = 'DataSample'
  __fields__ = [('vid', wp.vid, wp.required),
                ('creationDate', wp.timestamp, wp.required),
                ('status', DataSampleStatus, wp.required),
                ('action', Action, wp.required)]

class DataObject(wp.OmeroWrapper):
  OME_TABLE = 'DataObject'
  __fields__ = [('sample', DataSample, wp.required)]




