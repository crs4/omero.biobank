# BEGIN_COPYRIGHT
# END_COPYRIGHT

from bl.vl.kb import mimetypes
import wrapper as wp
from data_samples import MicroArrayMeasure
from vessels import Tube

class AffymetrixCelArrayType(wp.OmeroWrapper):

  OME_TABLE="AffymetrixCelArrayType"
  __enums__ = ["UNKNOWN", "GENOMEWIDESNP_6"]

class AffymetrixAssayType(wp.OmeroWrapper):

  OME_TABLE="AffymetrixAssayType"
  __enums__ = ["UNKNOWN", "GENOMEWIDESNP_6"]

class AffymetrixArray(Tube):

  OME_TABLE="AffymetrixArray"
  __fields__ = [('assayType', AffymetrixAssayType, wp.REQUIRED)]

  def __preprocess_conf__(self, conf):
    # to pacify Vessel constructor
    conf['initialVolume'] = 0.0
    conf['currentVolume'] = 0.0
    return super(AffymetrixArray, self).__preprocess_conf__(conf)




class AffymetrixCel(MicroArrayMeasure):

  OME_TABLE = 'AffymetrixCel'
  __fields__ = [('arrayType', AffymetrixCelArrayType, wp.REQUIRED),
                ('celID',     wp.STRING,              wp.OPTIONAL)]

