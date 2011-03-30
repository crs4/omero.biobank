from wrapper import OmeroWrapper
import omero.rtypes as ort

from bl.lib.sample.kb.drivers.omero.wrapper import OmeroWrapper
from bl.lib.sample.kb.drivers.omero.study import Study
import vl.lib.utils           as vlu
import vl.lib.utils.ome_utils as vluo

import bl.lib.individual.kb as kb

import time

from individual import Individual

class Enrollment(OmeroWrapper, kb.Enrollment):

  OME_TABLE = "Enrollment"

  def __init__(self, from_=None):
    ome_type = self.get_ome_type()
    if isinstance(from_, ome_type):
      ome_enrollment = from_
    else:
      ome_enrollment = ome_type()
      ome_enrollment.vid = ort.rstring(vlu.make_vid())
    super(Enrollment, self).__init__(ome_enrollment)

  def __handle_validation_errors__(self):
    if self.individual is None:
      raise kb.KBError("Enrollment individual can't be None")
    elif self.study is None:
      raise kb.KBError("Enrollment study can't be None")
    elif self.studyCode is None:
      raise kb.KBError("Enrollment studyCode can't be None")
    else:
      raise kb.KBError("unkwon error")

  def __setattr__(self, name, value):
    if name == 'individual':
      self.ome_obj.stIndUK = value.ome_obj.vid
      return setattr(self.ome_obj, name, value.ome_obj)
    elif name == 'study':
      self.ome_obj.stCodeUK = value.ome_obj.vid
      return setattr(self.ome_obj, name, value.ome_obj)
    else:
      return super(Enrollment, self).__setattr__(name, value)

  def __getattr__(self, name):
    if name == 'individual':
      return Individual(self.ome_obj.individual)
    elif name == 'study':
      return Study(self.ome_obj.study)
    else:
      return super(Enrollment, self).__getattr__(name)


