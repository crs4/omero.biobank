
# This is actually used in the meta class magic
import omero.model as om

from proxy_core import ProxyCore

from wrapper import ObjectFactory, MetaWrapper

import action
import vessels
import objects_collections
import data_samples
import actions_on_target
import individual


from genotyping import GenotypingAdapter
from modeling   import ModelingAdapter

KOK = MetaWrapper.__KNOWN_OME_KLASSES__

class Proxy(ProxyCore):
  """
  An omero driver for KB.

  """

  def __init__(self, host, user, passwd, session_keep_tokens=1):
    super(Proxy, self).__init__(host, user, passwd, session_keep_tokens)
    self.factory = ObjectFactory(proxy=self)
    #-- learn
    for k in KOK:
      klass = KOK[k]
      setattr(self, klass.get_ome_table(), klass)
    #-- setup adapters
    self.gadpt = GenotypingAdapter(self)
    self.madpt = ModelingAdapter(self)

  # MODELING related utility functions
  # ==================================

  def get_device(self, label):
    return self.madpt.get_device(label)

  def get_action_setup(self, label):
    return self.madpt.get_action_setup(label)

  def get_study(self, label):
    """
    Return the study object labeled 'label' or None if nothing matches 'label'.
    """
    return self.madpt.get_study(label)

  def get_objects(self, klass):
    return self.madpt.get_objects(klass)

  def get_enrolled(self, study):
    return self.madpt.get_enrolled(study)

  def get_enrollment(self, study, ind_label):
    return self.madpt.get_enrollment(study, ind_label)

  def get_vessels(self, klass=vessels.Vessel, content=None):
    return self.madpt.get_vessels(klass, content)

  def get_containers(self, klass=objects_collections.Container):
    return self.madpt.get_containers(klass)

  # GENOTYPING related utility functions
  # ====================================


  def delete_snp_marker_defitions_table(self):
    self.delete_table(self.gadpt.SNP_MARKER_DEFINITIONS_TABLE)

  def create_snp_marker_definitions_table(self):
    self.gadpt.create_snp_marker_definitions_table()

  def delete_snp_alignments_table(self):
    self.delete_table(self.gadpt.SNP_ALIGNMENT_TABLE)

  def create_snp_alignment_table(self):
    self.gadpt.create_snp_alignment_table()

  def delete_snp_markers_set_table(self):
    self.delete_table(self.gadpt.SNP_SET_DEF_TABLE)

  def create_snp_markers_set_table(self):
    self.gadpt.create_snp_markers_set_table()

  def delete_snp_set_table(self):
    self.delete_table(self.gadpt.SNP_SET_TABLE)

  def create_snp_set_table(self):
    self.gadpt.create_snp_set_table()

  def add_snp_marker_definitions(self, stream, op_vid, batch_size=50000):
    return self.gadpt.add_snp_marker_definitions(stream, op_vid, batch_size)

  def get_snp_marker_definitions(self, selector=None, batch_size=50000):
    return self.gadpt.get_snp_marker_definitions(selector, batch_size)

  def add_snp_alignments(self, stream, op_vid, batch_size=50000):
    return self.gadpt.add_snp_alignments(stream, op_vid, batch_size)

  def snp_markers_set_exists(self, maker, model, release):
    return self.madpt.snp_markers_set_exists(maker, model, release)

  def get_snp_markers_set(self, maker, model, release):
    return self.madpt.get_snp_markers_set(maker, model, release)

  def add_snp_markers_set(self, maker, model, release, op_vid):
    return self.gadpt.add_snp_markers_set(maker, model, release, op_vid)

  def fill_snp_markers_set(self, set_vid, stream, op_vid, batch_size=50000):
    return self.gadpt.fill_snp_markers_set(set_vid, stream, op_vid, batch_size)

  def create_gdo_repository(self, set_vid, N):
    return self.gadpt.create_gdo_repository(set_vid, N)


