import time

from bl.lib.sample.kb.drivers.omero.proxy_core import ProxyCore

from markers   import Markers
from gdo_repos import GdoRepos

class Proxy(ProxyCore):
  """
  """
  def __init__(self, host, user, passwd):
    self.markers = Markers(self)
    self.gdo_repos = GdoRepos(self)
    super(Proxy, self).__init__(host, user, passwd)

  #-- markers definition
  def add_snp_marker_definitions(self, stream, op_vid, batch_size=50000):
    return self.markers.extend_snp_definition_table(stream,
                                                    op_vid, batch_size=batch_size)

  def get_snp_marker_definitions(self, selector=None, batch_size=50000):
    """
    selector = "(source == 'affymetrix') & (context == 'GW6.0')"
    """
    return self.markers.get_snp_definition_table_rows(selector, batch_size=batch_size)

  #-- marker sets
  def create_snp_markers_set(self, maker, model, op_vid):
    return self.markers.extend_snp_set_def_table(maker, model, op_vid)

  def get_snp_markers_sets(self, selector=None, batch_size=50000):
    return self.markers.get_snp_set_def_table_rows(selector, batch_size)

  def fill_snp_markers_set(self, set_vid, stream, op_vid, batch_size=50000):
    return self.markers.extend_snp_set_table(set_vid, stream, op_vid, batch_size=batch_size)

  def get_snp_markers_set(self, selector=None, batch_size=50000):
    return self.markers.get_snp_set_table_rows(selector, batch_size)

  #-- alignment
  def add_snp_aligments(self, stream, op_vid, batch_size=50000):
    self.markers.extend_snp_alignment_table(stream, op_vid, batch_size)

  def get_snp_alignments(self, selector=None, batch_size=50000):
    return self.markers.get_snp_alignment_table_rows(selector, batch_size)

  #-- gdo
  def create_gdo_repository(self, set_vid, N):
    self.gdo_repos.create_repository(set_vid, N)

  def append_gdo(self, set_vid, probs, confidence, op_vid):
    return self.gdo_repos.append(set_vid, probs, confidence, op_vid)

  def get_gdo(self, set_vid, vid):
    return self.gdo_repos.get(set_vid, vid)

  def get_gdo_stream(self, set_vid, batch_size=10):
    return self.gdo_repos.get_gdo_stream(set_vid, batch_size=batch_size)


