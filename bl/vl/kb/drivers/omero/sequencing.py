import omero.model as om
import omero.rtypes as ort

import wrapper as wp
from data_samples import DataSample
from vessels import Tube
from objects_collections import Lane


class SequencerOutput(DataSample):
  OME_TABLE = 'SequencerOutput'
  __fields__ = []

class RawSeqDataSample(DataSample):  
  OME_TABLE = 'RawSeqDataSample'
  __fields__ = [('lane', Lane, wp.OPTIONAL)]


class SeqDataSample(DataSample):  
  OME_TABLE = 'SeqDataSample'
  __fields__ = [('sample', Tube, wp.OPTIONAL)]
