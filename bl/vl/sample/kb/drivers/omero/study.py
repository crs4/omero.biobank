from wrapper import OmeroWrapper
import omero.rtypes as ort
import bl.vl.utils           as vlu
import bl.vl.utils.ome_utils as vluo

import bl.vl.sample.kb as kb

import time

class Study(OmeroWrapper, kb.Study):

  OME_TABLE = "Study"

  def __init__(self, from_=None, label=None):
    ome_type = self.get_ome_type()
    if not (from_ is None):
      ome_obj = from_
    else:
      ome_obj = ome_type()
      ome_obj.vid = ort.rstring(vlu.make_vid())
      if label is not None:
        ome_obj.label = ort.rstring(label)
      ome_obj.startDate = vluo.time2rtime(time.time())
    super(Study, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.label is None:
      raise kb.KBError("study label can't be None")
    else:
      raise kb.KBError("a study with label %r already exists" %
                       self.label)

