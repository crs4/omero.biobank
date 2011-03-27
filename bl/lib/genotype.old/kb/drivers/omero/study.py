from wrapper import OmeroWrapper

import omero
import omero.model as om
import omero.rtypes as ort
import omero_sys_ParametersI as op
import omero_ServerErrors_ice  # magically adds exceptions to the omero module

import vl.lib.utils as vl_utils

import bl.lib.genotype.kb as kb

class Study(OmeroWrapper, kb.Study):

  OME_TABLE = "Study"

  def __init__(self, from_=None):
    ome_type = Study.get_ome_type()
    if isinstance(from_, ome_type):
      ome_study = from_
    else:
      label = from_
      ome_study = ome_type()
      ome_study.vid = ort.rstring(vl_utils.make_vid())
      if label is not None:
        ome_study.label = ort.rstring(label)
      ome_study.startDate = vl_utils.time2rtime(time.time())
    super(Study, self).__init__(ome_study)

