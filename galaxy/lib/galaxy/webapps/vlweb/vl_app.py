from galaxy.app import UniverseApplication as OrigUniverseApplication

#
from bl.vl.kb import KnowledgeBase as KB

class UniverseApplication(OrigUniverseApplication):
  def __init__(self, **kwargs):
    # FIXME just a quick and dirty fix...

    omero_host   = kwargs.get('omero_default_host')
    omero_user   = kwargs.get('omero_default_user')
    omero_passwd = kwargs.get('omero_default_passwd')
    self.kb = KB(driver='omero')(omero_host, omero_user, omero_passwd)

    super(UniverseApplication, self).__init__(**kwargs)

    #-- patch in omero specific configurations for future reference
    self.config.omero_default_host = kwargs.get('omero_default_host')
    self.config.omero_default_user = kwargs.get('omero_default_user')
    self.config.omero_default_passwd = kwargs.get('omero_default_passwd')
    self.config.vl_loglevel = kwargs.get('vl_loglevel', 'INFO')


  @property
  def known_studies(self):
    studies = self.kb.get_objects(self.kb.Study)
    if studies:
      return [(s.label, s.description) for s in studies]
    else:
      return []

  @property
  def known_scanners(self):
    scanners = self.kb.get_objects(self.kb.Scanner)
    if scanners:
      return [(s.id, s.label, s.physicalLocation) for s in scanners]
    else:
      return []

  @property
  def known_map_vid_source_types(self):
    from bl.vl.app.kb_query.map_vid import MapVIDApp
    sources = MapVIDApp.SUPPORTED_SOURCE_TYPES
    if sources:
      return sources
    else:
      return []
