from galaxy.app import UniverseApplication as OrigUniverseApplication

#
from bl.vl.kb import KnowledgeBase as KB

class UniverseApplication(OrigUniverseApplication):
  def __init__(self, **kwargs):
    # FIXME just a quick and dirty fix...
    self.kb = KB(driver='omero')('localhost', 'root', 'romeo')
    super(UniverseApplication, self).__init__(**kwargs)


  @property
  def known_studies(self):
    studies = self.kb.get_objects(self.kb.Study)
    if studies:
      return [(s.label, s.description) for s in studies]
    else:
      return []

