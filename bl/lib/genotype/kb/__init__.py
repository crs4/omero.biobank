"""
Interfaces for the application-side view of the object model.

Individual kb drivers should implement these interfaces.
"""


class KBError(Exception):
  pass


class Proxy(object):

  def __init__(self):
    raise NotImplementedError


class Study(object):

  def __init__(self):
    raise NotImplementedError


#---------------------------------------------------
#
# FIXME experimental implementation
#
#import bl.lib.genotype.kb.drivers.omero as omero
import vl.lib.utils as vlu
import sys
import itertools as it

driver_table = { 'omero' : 'bl.lib.genotype.kb.drivers.omero' }

class KnowledgeBase(object):
  def __init__(self, driver=None):
    self.marker_rs_label_to_vid = None
    self.marker_mask_to_vid     = None
    try:
      __import__(driver_table[driver])
      self.driver = sys.modules[driver_table[driver]]
    except KeyError, e:
      print 'Driver %s is unknown' % driver
      assert(False)

  def open(self, *argv):
    self.driver.open(*argv)

  def close(self):
    self.driver.close()

  def make_vid(self):
    return vlu.make_vid()

  def extend_snp_definition_table(self, stream, op_vid):
    self.driver.extend_snp_definition_table(stream, op_vid)

  def __get_markers_mappings(self):
    # FIXME: this is really gross...
    self.marker_rs_label_to_vid = {}
    self.marker_mask_to_vid     = {}
    mrks = self.driver.get_snp_definition_table_rows(selector=None)
    for x in mrks:
      self.marker_rs_label_to_vid[x[4]] = x[0]
      self.marker_mask_to_vid[x[5]] = x[0]

  def get_snp_vids(self, rs_labels=None, masks=None):
    if rs_labels and masks:
      raise ValueError('rs_labels and masks cannot be both set')
    if not self.marker_rs_label_to_vid:
        self.__get_markers_mappings()
    if rs_labels:
      return [ self.marker_rs_label_to_vid[l] for l in rs_labels]
    if masks:
      return [ self.marker_mask_to_vid[m] for m in masks]

  def create_new_snp_markers_set(self, maker, model, stream, op_vid, batch_size=50000):
    # FIXME: here we should handle actions
    op_vid = vlu.make_vid()
    set_vid = self.driver.extend_snp_set_def_table(maker, model, op_vid)
    self.driver.extend_snp_set_table(set_vid, stream, op_vid, batch_size)
    return set_vid

  def create_new_gdo_repository(self, set_vid):
    self.driver.create_gdo_repository(set_vid)

  def get_snp_marker_set_vid(self, maker, model):
    selector = "(maker=='%s')&(model=='%s')" % (maker, model)
    rows = self.driver.get_snp_set_def_table_rows(selector)
    if len(rows) == 0:
      raise ValueError('No marker set for %s.%s' % (maker, model))
    assert len(rows) <= 1
    return rows[0][0]

  #FIXME this inconsistent with the get_gdo_stream that returns dicts
  def append_gdo(self, set_vid, probs, confidence, op_vid):
    self.driver.append_gdo(set_vid, probs, confidence, op_vid)

  def get_gdo_stream(self, set_vid, batch_size=10):
    return self.driver.get_gdo_stream(set_vid, batch_size=batch_size)



  def get_gdos(self, individual_id):
    pass


