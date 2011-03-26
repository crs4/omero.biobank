from wrapper import OmeroWrapper

import vl.lib.utils           as vlu
import vl.lib.utils.ome_utils as vluo

import bl.lib.sample.kb as kb

import time

from result import Result

#------------------------------------------------------------
class Sample(Result, kb.Sample):

  OME_TABLE = "Sample"

#------------------------------------------------------------
class DataSample(Sample, kb.DataSample):

  OME_TABLE = "DataSample"

#------------------------------------------------------------
class BioSample(Sample, kb.BioSample):

  OME_TABLE = "BioSample"

  def __setattr__(self, name, value):
    if name == 'status':
      return setattr(self.ome_obj, name, value)
    else:
      return super(BioSample, self).__setattr__(name, value)


#------------------------------------------------------------
class BloodSample(BioSample, kb.BloodSample):

  OME_TABLE = "BloodSample"

#------------------------------------------------------------
class DNASample(BioSample, kb.DNASample):

  OME_TABLE = "DNASample"

#------------------------------------------------------------
class SerumSample(BioSample, kb.SerumSample):

  OME_TABLE = "SerumSample"
