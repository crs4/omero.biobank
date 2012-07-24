import omero.model as om
import omero.rtypes as ort

import wrapper as wp
from data_samples import DataSample
from vessels import Vessel
from objects_collections import Lane


class SequencerOutput(DataSample):
  OME_TABLE = 'SequencerOutput'
  __fields__ = []

class RawSeqDataSample(DataSample):
  
  OME_TABLE = 'RawSeqDataSample'
  __fields__ = [('read', wp.INT, wp.REQUIRED),
                ('lane', Lane, wp.REQUIRED)]


class SeqDataSample(DataSample):
  
  OME_TABLE = 'SeqDataSample'
  __fields__ = [('sample', Vessel, wp.REQUIRED)]
