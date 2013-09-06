from bl.vl.kb.galaxy.core_wrappers import Workflow as CoreWorkflow
from bl.vl.kb.galaxy.core_wrappers import History as CoreHistory

class Workflow(CoreWorkflow):
    def __init__(self, wf_id, wf_dict):
        super(Workflow, self).__init__(wf_dict)
        setattr(self.core, 'id', wf_id)

    def clone(self):
        return self.__class__(None, self.core.wrapped)
        
    @property
    def id(self):
        return self.core.id

    def __eq__(self, other):
        return  (self.id == other.id 
                 and super(Workflow, self).__eq__(other))
     

class History(CoreHistory):
    pass



    
        
