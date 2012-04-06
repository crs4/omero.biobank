# BEGIN_COPYRIGHT
# END_COPYRIGHT
import os

from galaxy.app import UniverseApplication as OrigUniverseApplication
from bl.vl.kb import KnowledgeBase as KB

def ome_env_variable(name):
    if os.environ.has_key(name):
        return os.environ[name]
    else:
        msg = 'Can\'t use default parameter, environment variable %s does not exist' % name
        raise ValueError(msg)

def ome_host():
    return ome_env_variable('OME_HOST')

def ome_user():
    return ome_env_variable('OME_USER')

def ome_passwd():
    return ome_env_variable('OME_PASSWD')

class UniverseApplication(OrigUniverseApplication):
  
  def __init__(self, **kwargs):
    omero_host = ome_host()
    omero_user = ome_user()
    omero_passwd = ome_passwd()
    self.kb = KB(driver='omero')(omero_host, omero_user, omero_passwd)
    super(UniverseApplication, self).__init__(**kwargs)
    self.config.omero_default_host = kwargs.get('omero_default_host')
    self.config.omero_default_user = kwargs.get('omero_default_user')
    self.config.omero_default_passwd = kwargs.get('omero_default_passwd')
    self.config.vl_loglevel = kwargs.get('vl_loglevel', 'INFO')
    self.config.vl_import_enabled_users = kwargs.get('vl_import_enabled_users')

  @property
  def known_studies(self):
    studies = self.kb.get_objects(self.kb.Study)
    if studies:
      return [(s.label, s.description or '') for s in studies]
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
