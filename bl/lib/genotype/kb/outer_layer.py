import vl.lib.utils as vlu


class InnerLayer(object):

  DRIVER_CLASS = None

  def __init__(self, host, user, passwd):
    self.driver = self.DRIVER_CLASS(host, user, passwd)
    self.marker_rs_label_to_vid = None
    self.marker_mask_to_vid     = None

  def make_vid(self):
    return vlu.make_vid()

  #-- marker def
  def add_snp_marker_definitions(self, stream, op_vid):
    return self.driver.add_snp_marker_definitions(stream, op_vid)

  def get_snp_marker_definitions(self, selector):
    return self.driver.get_snp_marker_definitions(selector)

  def __get_markers_mappings(self):
    # FIXME: this is really gross...
    self.marker_rs_label_to_vid = {}
    self.marker_mask_to_vid     = {}
    mrks = self.get_snp_marker_definitions(selector=None)
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

  #-- marker sets
  def create_snp_markers_set(self, maker, model, stream,
                             op_vid, batch_size=50000):
    set_vid = self.driver.create_snp_markers_set(maker, model, op_vid)
    N = self.driver.fill_snp_markers_set(set_vid, stream, op_vid, batch_size)
    self.driver.create_gdo_repository(set_vid, N)
    return set_vid

  def get_snp_markers_sets(self, selector=None, batch_size=50000):
    return self.driver.get_snp_markers_sets(selector, batch_size)

  def get_snp_markers_set(self, selector=None, batch_size=50000):
    return self.driver.get_snp_markers_set(selector, batch_size)

  #-- alignment
  def add_snp_alignments(self, stream, op_vid, batch_size=50000):
    return self.driver.add_snp_aligments(stream, op_vid, batch_size)

  def get_snp_alignments(self, selector=None, batch_size=50000):
    return self.driver.get_snp_alignments(selector, batch_size)

  #-- gdo
  # def create_gdo_repository(self, set_vid):
  #   #FIXME: this is kind of stupid...
  #   mrks = self.driver.get_snp_markers_set('(vid=="%s")' % set_vid)
  #   if mrks is None:
  #     raise ValueError('Unknown set %s' % set_vid)
  #   return self.driver.create_gdo_repository(set_vid, mrks.shape[0])

  def get_snp_marker_set_vid(self, maker, model):
    selector = "(maker=='%s')&(model=='%s')" % (maker, model)
    rows = self.driver.get_snp_markers_set_definitions(selector)
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


#----------------------------------------------------------------------
def OuterLayer(driver_class):
  class Wrapper(InnerLayer):
    DRIVER_CLASS = driver_class
  return Wrapper




