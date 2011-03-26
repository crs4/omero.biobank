from wrapper import OmeroWrapper
import omero.rtypes as ort
import vl.lib.utils           as vlu
import vl.lib.utils.ome_utils as vluo

import bl.lib.sample.kb as kb

import time

class Study(OmeroWrapper, kb.Study):

  OME_TABLE = "Study"

  def __init__(self, from_=None):
    ome_type = Study.get_ome_type()
    if isinstance(from_, ome_type):
      ome_study = from_
    else:
      label = from_
      ome_study = ome_type()
      ome_study.vid = ort.rstring(vlu.make_vid())
      if label is not None:
        ome_study.label = ort.rstring(label)
      ome_study.startDate = vluo.time2rtime(time.time())
    super(Study, self).__init__(ome_study)

  def __handle_validation_errors__(self):
    if self.label is None:
      raise kb.KBError("study label can't be None")
    else:
      raise kb.KBError("a study with label %r already exists" %
                       self.label)

