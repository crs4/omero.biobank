import omero.rtypes as ort
import vl.lib.utils           as vlu
import vl.lib.utils.ome_utils as vluo

import bl.lib.individual.kb as kb
from bl.lib.sample.kb.drivers.omero.wrapper import OmeroWrapper

import time

class Individual(OmeroWrapper, kb.Individual):

  OME_TABLE = "Individual"

  def __init__(self, from_=None):
    ome_type = self.get_ome_type()
    if isinstance(from_, ome_type):
      ome_individual = from_
    else:
      gender = from_
      ome_individual = ome_type()
      ome_individual.vid = ort.rstring(vlu.make_vid())
      if gender is not None:
        ome_individual.gender = gender
    super(Individual, self).__init__(ome_individual)

  def __handle_validation_errors__(self):
    if self.gender is None:
      raise kb.KBError("Individual gender can't be None")
    else:
      raise kb.KBError("unkwon error")

  def __setattr__(self, name, value):
    if name == 'father':
      return setattr(self.ome_obj, name, value.ome_obj)
    elif name == 'mother':
      return setattr(self.ome_obj, name, value.ome_obj)
    elif name == 'gender':
      return setattr(self.ome_obj, name, value)
    else:
      return super(Individual, self).__setattr__(name, value)

  def __getattr__(self, name):
    if name == 'father':
      return Individual(self.ome_obj.father)
    elif name == 'mother':
      return Individual(self.ome_obj.mother)
    else:
      return super(Individual, self).__getattr__(name)


