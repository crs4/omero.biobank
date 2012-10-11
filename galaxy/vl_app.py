# BEGIN_COPYRIGHT
# END_COPYRIGHT
import os

from galaxy.app import UniverseApplication as OrigUniverseApplication
from bl.vl.kb import KnowledgeBase as KB
import bl.vl.kb.drivers.omero.utils as vlu

class UniverseApplication(OrigUniverseApplication):
  
  def __init__(self, **kwargs):
    omero_host = vlu.ome_host()
    omero_user = vlu.ome_user()
    omero_passwd = vlu.ome_passwd()
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

  @property
  def known_marker_sets(self):
    msets = self.kb.get_objects(self.kb.SNPMarkersSet)
    if msets:
      return [(ms.id, ms.label) for ms in msets]
    else:
      return []

  @property
  def known_data_collections(self):
    dcolls = self.kb.get_objects(self.kb.DataCollection)
    if dcolls:
      return [(dc.id, dc.label) for dc in dcolls]
    else:
      return []

  @property
  def known_vessels_collections(self):
    vcolls = self.kb.get_objects(self.kb.VesselsCollection)
    if vcolls:
      return [(vc.id, vc.label) for vc in vcolls]
    else:
      return []

  @property
  def known_titer_plates(self):
    plates = self.kb.get_objects(self.kb.TiterPlate)
    if plates:
      return [(pl.barcode, pl.label) for pl in plates if pl.barcode]
    else:
      return []

  @property
  def known_vessel_status(self):
    vstatus = self.kb.get_objects(self.kb.VesselStatus)
    if vstatus:
      return [(v.omero_id, v.enum_label()) for v in vstatus]
    else:
      return []

  @property
  def known_data_sample_status(self):
    dsstatus = self.kb.get_objects(self.kb.DataSampleStatus)
    if dsstatus:
      return [(ds.omero_id, ds.enum_label()) for ds in dsstatus]
    else:
      return []

  @property
  def known_hardware_devices(self):
    hdev = self.kb.get_objects(self.kb.HardwareDevice)
    if hdev:
      return [(h.id, h.label) for h in hdev]
    else:
      return []

  @property
  def known_software_devices(self):
    sdev = self.kb.get_objects(self.kb.SoftwareDevice)
    if sdev:
      return [(s.id, s.label) for s in sdev]
    else:
      return []

  @property
  def known_devices(self):
    dev = self.kb.get_objects(self.kb.Device)
    if dev:
      return [(d.id, d.label) for d in dev]
    else:
      return []
