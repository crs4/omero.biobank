import bl.lib.individual.kb as kb
from bl.lib.sample.kb.drivers.omero.action import Action

#----------------------------------------------------------------------
class ActionOnIndividual(Action, kb.ActionOnIndividual):

  OME_TABLE = "ActionOnIndividual"

  def __handle_validation_errors__(self):
    if self.target is None:
      raise kb.KBError("ActionOnIndividual target can't be None")
    else:
      super(ActionOnIndividual, self).__handle_validation_errors__()

  def __setattr__(self, name, value):
    if name == 'target':
      return setattr(self.ome_obj, name, value.ome_obj)
    else:
      return super(ActionOnIndividual, self).__setattr__(name, value)

  def __getattr__(self, name):
    if name == 'target':
      return Sample(self.ome_obj.device)
    else:
      return super(ActionOnIndividual, self).__getattr__(name)

