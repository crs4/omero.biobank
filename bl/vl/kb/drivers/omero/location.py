import omero.model as om
import omero.rtypes as ort

import bl.vl.utils as vu
import bl.vl.utils.ome_utils as vou
import wrapper as wp

from utils import assign_vid

class Location(wp.OmeroWrapper):
  OME_TABLE = 'Location'
  __fields__ = [('vid',  wp.VID, wp.REQUIRED),
                ('name', wp.STRING, wp.REQUIRED),
                ('istatCode', wp.STRING, wp.REQUIRED),
                ('lastUpdate', wp.TIMESTAMP, wp.REQUIRED),
                ('ceaseDate', wp.TIMESTAMP, wp.OPTIONAL)]

  def __preprocess_conf__(self, conf):
    return assign_vid(conf)

class State(Location):
  OME_TABLE = 'State'
  __fields__ = [('landRegisterCode', wp.STRING, wp.OPTIONAL)]

class Region(Location):
  OME_TABLE = 'Region'
  __fields__ = []

class City(Location):
  OME_TABLE = 'City'
  __fields__ = [('zipCode', wp.STRING, wp.OPTIONAL),
                ('landRegisterCode', wp.STRING, wp.OPTIONAL),
                ('uslCode', wp.STRING, wp.OPTIONAL),
                ('region', Region, wp.OPTIONAL),
                ('districtLabel', wp.STRING, wp.OPTIONAL)]

