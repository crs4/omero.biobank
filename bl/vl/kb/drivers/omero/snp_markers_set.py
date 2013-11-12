# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
FIXME
"""

from utils import make_unique_key
import wrapper as wp

class SNPMarkersSet(wp.OmeroWrapper):

  OME_TABLE = 'SNPMarkersSet'

  __fields__ = [('label', wp.STRING, wp.REQUIRED),
                ('maker', wp.STRING, wp.REQUIRED),
                ('model', wp.STRING, wp.REQUIRED),
                ('release', wp.STRING, wp.REQUIRED),
                ('markersSetVID', wp.VID, wp.REQUIRED),
                ('snpMarkersSetUK', wp.STRING, wp.REQUIRED)]
  __do_not_serialize__ = ['snpMarkersSetUK']

  def __preprocess_conf__(self, conf):
    if not 'snpMarkersSetUK' in conf:
      conf['snpMarkersSetUK'] = make_unique_key(conf['maker'], conf['model'],
                                                conf['release'])
    return conf

  def __cleanup__(self):
    self.proxy.genomics.delete_markers_array_tables(self.id)

  @property
  def id(self):
    return self.markersSetVID

  @property
  def vid(self):
      return self.id


  def __update_constraints__(self):
    uk = make_unique_key(self.maker, self.model, self.release)
    setattr(self.ome_obj, 'snpMarkersSetUK',
            self.to_omero(self.__fields__['snpMarkersSetUK'][0], uk))

