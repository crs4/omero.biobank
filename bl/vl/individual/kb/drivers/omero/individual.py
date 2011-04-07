import omero.rtypes as ort
import vl.lib.utils           as vlu
import vl.lib.utils.ome_utils as vluo

import bl.lib.individual.kb as kb
from bl.lib.sample.kb.drivers.omero.wrapper import OmeroWrapper

import time

class Individual(OmeroWrapper, kb.Individual):

  OME_TABLE = "Individual"

  def __setup__(self, ome_obj, gender):
    ome_obj.vid = ort.rstring(vlu.make_vid())
    ome_obj.gender = gender


  def __init__(self, from_=None, gender=None):
    ome_type = self.get_ome_type()
    if not from_ is None:
      ome_obj = from_
    else:
      if gender is None:
        raise ValueError('Individual gender cannot be None')
      ome_obj = ome_type()
      self.__setup__(ome_obj, gender)
    super(Individual, self).__init__(ome_obj)

  def __handle_validation_errors__(self):
    if self.gender is None:
      raise kb.KBError("Individual gender can't be None")
    else:
      super(Individual, self).__handle_validation_errors__()

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


